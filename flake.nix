{
  description = "criome - Spartan BLS-signature authentication and attestation daemon";

  inputs = {
    nixpkgs.url = "github:LiGoldragon/nixpkgs?ref=main";

    fenix.url = "github:nix-community/fenix";
    fenix.inputs.nixpkgs.follows = "nixpkgs";

    crane.url = "github:ipetkov/crane";
  };

  outputs =
    {
      self,
      nixpkgs,
      fenix,
      crane,
    }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
      ];
      forSystems = function: nixpkgs.lib.genAttrs systems (system: function system);

      mkContext =
        system:
        let
          pkgs = import nixpkgs { inherit system; };
          toolchain = fenix.packages.${system}.stable.withComponents [
            "cargo"
            "rustc"
            "rustfmt"
            "clippy"
            "rust-src"
          ];
          craneLib = (crane.mkLib pkgs).overrideToolchain toolchain;
          src = pkgs.lib.cleanSourceWith {
            src = ./.;
            filter =
              path: type: (craneLib.filterCargoSources path type) || pkgs.lib.hasSuffix ".schema" (toString path);
          };
          cargoVendorDir = craneLib.vendorCargoDeps { inherit src; };
          commonArgs = {
            inherit src cargoVendorDir;
            strictDeps = true;
          };
          cargoArtifacts = craneLib.buildDepsOnly commonArgs;
        in
        {
          inherit
            pkgs
            toolchain
            craneLib
            src
            commonArgs
            cargoArtifacts
            ;
        };
    in
    {
      packages = forSystems (
        system:
        let
          context = mkContext system;
        in
        {
          default = context.craneLib.buildPackage (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              cargoExtraArgs = "--features nota-text";
              meta.mainProgram = "criome";
            }
          );
        }
      );

      checks = forSystems (
        system:
        let
          context = mkContext system;
        in
        {
          default = context.craneLib.cargoTest (context.commonArgs // { inherit (context) cargoArtifacts; });
          build = context.craneLib.cargoBuild (context.commonArgs // { inherit (context) cargoArtifacts; });
          test = context.craneLib.cargoTest (context.commonArgs // { inherit (context) cargoArtifacts; });
          daemon-skeleton = context.craneLib.cargoTest (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              cargoTestExtraArgs = "--test daemon_skeleton";
            }
          );
          test-nota-text = context.craneLib.cargoTest (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              cargoTestExtraArgs = "--features nota-text --all-targets";
            }
          );
          criome-uses-kameo-not-ractor = context.pkgs.runCommand "criome-uses-kameo-not-ractor" { } ''
            set -euo pipefail

            ${context.pkgs.gnugrep}/bin/grep -F 'kameo' ${./Cargo.toml} > /dev/null
            ! ${context.pkgs.gnugrep}/bin/grep -R -E '(^|[^[:alnum:]_])ractor([^[:alnum:]_]|$)' ${./Cargo.toml} ${./src}
            touch "$out"
          '';
          criome-signal-criome-contract-boundary =
            context.pkgs.runCommand "criome-signal-criome-contract-boundary" { }
              ''
                set -euo pipefail

                ${context.pkgs.gnugrep}/bin/grep -F 'signal-criome' ${./Cargo.toml} > /dev/null
                ! ${context.pkgs.gnugrep}/bin/grep -F 'signal       =' ${./Cargo.toml}
                touch "$out"
              '';
          criome-meta-session-architecture = context.pkgs.runCommand "criome-meta-session-architecture" { } ''
            set -euo pipefail

            ${context.pkgs.gnugrep}/bin/grep -F 'Meta-session bytes are encrypted' ${./ARCHITECTURE.md} > /dev/null
            ${context.pkgs.gnugrep}/bin/grep -F 'ECDH' ${./ARCHITECTURE.md} > /dev/null
            ! ${context.pkgs.gnugrep}/bin/grep -F 'Plaintext passphrase over the owner socket is acceptable' ${./ARCHITECTURE.md}
            touch "$out"
          '';
          criome-authorization-slots-are-store-minted =
            context.pkgs.runCommand "criome-authorization-slots-are-store-minted" { }
              ''
                set -euo pipefail

                ${context.pkgs.gnugrep}/bin/grep -F 'authorization_next_slot' ${./src}/tables.rs > /dev/null
                ${context.pkgs.gnugrep}/bin/grep -F 'Authorization request slots are durable store-minted' ${./ARCHITECTURE.md} > /dev/null
                ! ${context.pkgs.gnugrep}/bin/grep -R -E 'slot_for_digest|request_digest\.as_str\(\)|AuthorizationRequestSlot::new\([^)]*digest' ${./src}
                touch "$out"
              '';
          criome-authorization-expiry-and-replay-guard =
            context.pkgs.runCommand "criome-authorization-expiry-and-replay-guard" { }
              ''
                set -euo pipefail

                ${context.pkgs.gnugrep}/bin/grep -F 'authorization_replay_nonces' ${./src}/tables.rs > /dev/null
                ${context.pkgs.gnugrep}/bin/grep -F 'AuthorizationReplayAttempted' ${./src}/error.rs > /dev/null
                ${context.pkgs.gnugrep}/bin/grep -F 'expired_authorization_records_expired_state_instead_of_signing' ${./tests}/daemon_skeleton.rs > /dev/null
                ${context.pkgs.gnugrep}/bin/grep -F 'authorization_replay_nonce_rejects_changed_digest_reuse' ${./tests}/daemon_skeleton.rs > /dev/null
                ${context.pkgs.gnugrep}/bin/grep -F 'Authorization expiry and replay guard' ${./ARCHITECTURE.md} > /dev/null
                touch "$out"
              '';
          fmt = context.craneLib.cargoFmt { inherit (context) src; };
          clippy = context.craneLib.cargoClippy (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              cargoClippyExtraArgs = "--all-targets -- -D warnings";
            }
          );
          clippy-nota-text = context.craneLib.cargoClippy (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              cargoClippyExtraArgs = "--features nota-text --all-targets -- -D warnings";
            }
          );
          doc = context.craneLib.cargoDoc (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              RUSTDOCFLAGS = "-D warnings";
            }
          );
        }
      );

      apps = forSystems (system: {
        default = {
          type = "app";
          program = "${self.packages.${system}.default}/bin/criome";
        };
        daemon = {
          type = "app";
          program = "${self.packages.${system}.default}/bin/criome-daemon";
        };
      });

      devShells = forSystems (
        system:
        let
          context = mkContext system;
        in
        {
          default = context.pkgs.mkShell {
            name = "criome";
            packages = [
              context.toolchain
              context.pkgs.jujutsu
              context.pkgs.pkg-config
            ];
          };
        }
      );
    };
}
