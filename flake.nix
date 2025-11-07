{
  description = "Development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.05";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachSystem [
      "x86_64-linux"
    ] (system:
      let
        overlays = [ ];

        pkgs = import nixpkgs {
          inherit system overlays;
          config = {
            allowUnfree = true;
            permittedInsecurePackages = [
              "dotnet-sdk-6.0.428"
            ];
          };
        };

        projectSolution = "clickhouse-pure.slnx";

        combinedDotnet =
          pkgs.dotnetCorePackages.combinePackages [
            pkgs.dotnetCorePackages.sdk_6_0
            pkgs.dotnetCorePackages.sdk_8_0
            pkgs.dotnetCorePackages.sdk_9_0
          ];

        projectRootInit = ''
          if [ -z "''${PROJECT_ROOT-}" ]; then
            if project_root="$(${pkgs.git}/bin/git rev-parse --show-toplevel 2>/dev/null)"; then
              export PROJECT_ROOT="$project_root"
            else
              export PROJECT_ROOT="$PWD"
            fi
          fi
        '';

        dotnetEnvBlock = ''
          export DOTNET_ROOT=${combinedDotnet}/share/dotnet
          export DOTNET_CLI_TELEMETRY_OPTOUT=1
          export DOTNET_SKIP_FIRST_TIME_EXPERIENCE=1
          export DOTNET_NOLOGO=1
          export DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=0
          export DOTNET_CLI_HOME="$PROJECT_ROOT/.dotnet"
          export NUGET_PACKAGES="$PROJECT_ROOT/.nuget/packages"
          mkdir -p "$DOTNET_CLI_HOME" "$NUGET_PACKAGES"
          export PATH="$DOTNET_CLI_HOME/tools:$PATH"
          export LD_LIBRARY_PATH=${pkgs.lib.makeLibraryPath [ pkgs.openssl pkgs.icu pkgs.stdenv.cc.cc ]}
          export NIX_LD="${pkgs.stdenv.cc.bintools.dynamicLinker}"
        '';

        riderBase = pkgs.jetbrains.rider.override {
          vmopts = ''
            -Xms4096m
            -Xmx10G
            -Dawt.toolkit.name=WLToolkit
          '';
        };

        riderWithPlugins = pkgs.jetbrains.plugins.addPlugins riderBase [
          "ideavim"
          "string-manipulation"
        ];

        dotnetFhs = pkgs.buildFHSEnv {
          name = "dotnet-rider-env";
          targetPkgs = pkgs: with pkgs; [
            bash
            coreutils
            git
            cacert
            openssl
            icu
            stdenv.cc.cc
            combinedDotnet
            netcoredbg
            csharp-ls
            protobuf
            grpc
          ];
          profile = ''
            ${projectRootInit}
            ${dotnetEnvBlock}
          '';
        };

        riderLauncher = pkgs.writeShellApplication {
          name = "open-rider";
          runtimeInputs = [
            pkgs.git
            combinedDotnet
            pkgs.csharp-ls
            pkgs.netcoredbg
            dotnetFhs
            riderWithPlugins
          ];
          text = ''
            set -euo pipefail
            ${projectRootInit}
            ${dotnetEnvBlock}
            export IDE_USE_WAYLAND=1
            export GDK_BACKEND=wayland
            export QT_QPA_PLATFORM=wayland
            export XDG_SESSION_TYPE=wayland
            log_dir="$PROJECT_ROOT/.logs"
            log_file="$log_dir/rider.log"
            pid_file="$log_dir/rider.pid"
            mkdir -p "$log_dir"
            if [ -f "$pid_file" ]; then
              existing_pid="$(cat "$pid_file" 2>/dev/null || true)"
              if [ -n "$existing_pid" ] && kill -0 "$existing_pid" 2>/dev/null; then
                printf 'Rider already running with PID %s\n' "$existing_pid"
                exit 0
              fi
            fi
            printf 'Starting Rider in background, logging to %s\n' "$log_file"
            nohup ${dotnetFhs}/bin/dotnet-rider-env ${riderWithPlugins}/bin/rider "$PROJECT_ROOT/${projectSolution}" "$@" >>"$log_file" 2>&1 &
            rider_pid="$!"
            printf '%s\n' "$rider_pid" > "$pid_file"
            printf 'Rider PID: %s\n' "$rider_pid"
          '';
        };

        riderStopper = pkgs.writeShellApplication {
          name = "stop-rider";
          runtimeInputs = [
            pkgs.git
            pkgs.coreutils
          ];
          text = ''
            set -euo pipefail
            ${projectRootInit}
            log_dir="$PROJECT_ROOT/.logs"
            log_file="$log_dir/rider.log"
            pid_file="$log_dir/rider.pid"
            mkdir -p "$log_dir"

            log() {
              local msg="$1"
              printf '%s\n' "$msg"
              printf '%s\n' "$msg" >>"$log_file"
            }

            if [ ! -f "$pid_file" ]; then
              log "No Rider PID file found at $pid_file; nothing to stop."
              exit 0
            fi

            rider_pid="$(cat "$pid_file" 2>/dev/null || true)"

            if [ -z "$rider_pid" ]; then
              log "Rider PID file $pid_file is empty; removing."
              rm -f "$pid_file"
              exit 0
            fi

            if kill -0 "$rider_pid" 2>/dev/null; then
              log "Stopping Rider (PID $rider_pid)..."
              kill "$rider_pid"
              rm -f "$pid_file"
              log "Rider stopped."
            else
              log "No running Rider process with PID $rider_pid; removing stale PID file."
              rm -f "$pid_file"
            fi
          '';
        };

        nvimLauncher = pkgs.writeShellApplication {
          name = "nvim-dotnet";
          runtimeInputs = [
            pkgs.git
            combinedDotnet
            pkgs.csharp-ls
            pkgs.netcoredbg
            pkgs.neovim
          ];
          text = ''
            set -euo pipefail
            ${projectRootInit}
            ${dotnetEnvBlock}
            exec ${pkgs.neovim}/bin/nvim "$@"
          '';
        };

        devlopLauncher = pkgs.writeShellApplication {
          name = "devlop";
          runtimeInputs = [
            pkgs.nix
            pkgs.git
          ];
          text = ''
            set -euo pipefail
            ${projectRootInit}
            exec nix develop "$PROJECT_ROOT" "$@"
          '';
        };

        releaseTagger = pkgs.writeShellApplication {
          name = "tag-release";
          runtimeInputs = [
            pkgs.git
            pkgs.coreutils
          ];
          text = ''
            set -euo pipefail
            ${projectRootInit}

            usage() {
              cat <<'USAGE'
Usage: tag-release <tag> <description>

Creates an annotated git tag with the provided description and pushes it to the remote.
Set GIT_REMOTE to override the default remote (origin).
USAGE
            }

            if [ "$#" -lt 2 ]; then
              usage
              exit 1
            fi

            tag="$1"
            shift
            message="$*"

            if [ -z "$message" ]; then
              printf 'error: description must not be empty\n' >&2
              usage
              exit 1
            fi

            remote="''${GIT_REMOTE:-origin}"

            cd "$PROJECT_ROOT"

            if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
              printf 'error: %s is not inside a git repository\n' "$PROJECT_ROOT" >&2
              exit 1
            fi

            if [ -n "$(git status --porcelain --untracked-files=no)" ]; then
              printf 'error: working tree has uncommitted changes; please commit or stash before tagging\n' >&2
              exit 1
            fi

            if git rev-parse -q --verify "refs/tags/$tag" >/dev/null 2>&1; then
              printf 'error: tag %s already exists\n' "$tag" >&2
              exit 1
            fi

            printf 'Creating annotated tag %s\n' "$tag"
            git tag -a "$tag" -m "$message"

            printf 'Pushing tag %s to %s\n' "$tag" "$remote"
            git push "$remote" "$tag"

            printf 'Tag %s created and pushed successfully.\n' "$tag"
          '';
        };

        devPackages = (with pkgs; [
          combinedDotnet
          netcoredbg
          csharp-ls
          protobuf
          grpc
          git
          openssl
          icu
          pkg-config
          bashInteractive
          gnupg
          unzip
          zip
          cacert
          dotnetFhs
          riderWithPlugins
          riderLauncher
          riderStopper
          neovim
        ]) ++ [ releaseTagger ];

        devShell = pkgs.mkShell {
          packages = devPackages;
          shellHook = ''
            ${projectRootInit}
            ${dotnetEnvBlock}
            if [ -f "$PROJECT_ROOT/.config/dotnet-tools.json" ]; then
              dotnet tool restore --disable-parallel --verbosity quiet
            fi
          '';
        };
      in {
        apps = {
          devlop = {
            type = "app";
            program = "${devlopLauncher}/bin/devlop";
          };
          rider = {
            type = "app";
            program = "${riderLauncher}/bin/open-rider";
          };
          "rider-stop" = {
            type = "app";
            program = "${riderStopper}/bin/stop-rider";
          };
          nvim = {
            type = "app";
            program = "${nvimLauncher}/bin/nvim-dotnet";
          };
          "tag-release" = {
            type = "app";
            program = "${releaseTagger}/bin/tag-release";
          };
        };

        devShells = {
          default = devShell;
          devlop = devShell;
        };
      });
}

