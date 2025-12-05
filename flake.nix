{
  description = ".NET development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs = { self, nixpkgs }:
    let
      system = "x86_64-linux";
      overlays = [ ];

      pkgs = import nixpkgs {
        inherit system overlays;
        config = {
          allowUnfree = true;
        };
      };

      combinedDotnet = pkgs.dotnetCorePackages.combinePackages (with pkgs.dotnetCorePackages; [
        sdk_9_0
      ]);

      dotnetEnvironment = ''
        WORKSPACE_DIR="''${WORKSPACE_DIR:-$PWD}"
        export WORKSPACE_DIR
        export DOTNET_ROOT=${combinedDotnet}/share/dotnet
        export DOTNET_CLI_TELEMETRY_OPTOUT=1
        export DOTNET_SKIP_FIRST_TIME_EXPERIENCE=1
        export DOTNET_NOLOGO=1
        export DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=0
        export DOTNET_CLI_HOME="$WORKSPACE_DIR/.dotnet"
        export NUGET_PACKAGES="$WORKSPACE_DIR/.nuget/packages"
        export NUGET_CONFIG_PATH="$WORKSPACE_DIR/NuGet.config"
        export="$WORKSPACE_DIR/.nuget/packages"
        export PATH="$DOTNET_CLI_HOME/tools:$PATH"
        export LD_LIBRARY_PATH=${pkgs.lib.makeLibraryPath [ pkgs.openssl pkgs.icu pkgs.stdenv.cc.cc ]}
        export NIX_LD="${pkgs.stdenv.cc.bintools.dynamicLinker}"
        mkdir -p "$DOTNET_CLI_HOME" "$NUGET_PACKAGES"
      '';

      workspaceDetection = ''
        resolve_workspace_dir() {
          if [ -n "''${WORKSPACE_DIR:-}" ]; then
            printf "%s\n" "$WORKSPACE_DIR"
            return 0
          fi
          local candidate="$PWD"
          while [ "$candidate" != "/" ]; do
            if [ -d "$candidate/.git" ]; then
              printf "%s\n" "$candidate"
              return 0
            fi
            candidate=$(dirname "$candidate")
          done
          printf "%s\n" "$PWD"
        }
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
        profile = dotnetEnvironment;
      };

      riderLauncher = pkgs.writeShellApplication {
        name = "rider";
        runtimeInputs = [ riderWithPlugins dotnetFhs pkgs.coreutils ];
        text = ''
          set -euo pipefail
          ${workspaceDetection}
          WORKSPACE_DIR=$(resolve_workspace_dir)
          export WORKSPACE_DIR
          log_dir="$PWD/.nix-rider"
          mkdir -p "$log_dir"
          log_file="$log_dir/rider.log"
          nohup ${dotnetFhs}/bin/dotnet-rider-env ${riderWithPlugins}/bin/rider "$@" >"$log_file" 2>&1 &
          pid=$!
          disown "$pid"
          echo "Rider started in background (PID $pid). Logs: $log_file"
        '';
      };

      commonPackages = with pkgs; [
        combinedDotnet
        netcoredbg
        csharp-ls
        protobuf
        grpc
        openssl
        icu
        pkg-config
        git
        bashInteractive
        gnupg
        unzip
        zip
        cacert
        dotnetFhs
        riderLauncher
      ];
    in {
      packages.${system} = {
        default = riderLauncher;
        rider = riderLauncher;
      };

      apps.${system}.default = {
        type = "app";
        program = "${riderLauncher}/bin/rider";
      };

      devShells.${system}.default = pkgs.mkShell {
        packages = commonPackages;
        shellHook = ''
          export WORKSPACE_DIR="''${WORKSPACE_DIR:-$PWD}"
          ${dotnetEnvironment}
          if [ -f .config/dotnet-tools.json ]; then
            dotnet tool restore --disable-parallel --verbosity quiet
          fi
        '';
      };
    };
}
