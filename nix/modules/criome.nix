# criome.nix — the minimal NixOS module that runs the criome daemon as a
# systemd service from a PRE-ENCODED binary rkyv startup, honoring the
# one-rkyv-arg / no-flags daemon discipline.
#
# This is the deploy-path sibling to the router's `message-router.nix`. Both
# follow the same shape: the module authors the daemon's typed configuration as
# a single positional NOTA record, an `ExecStartPre` step seals that NOTA into
# the rkyv artifact the daemon consumes, and `ExecStart` launches
# `criome-daemon <config.rkyv>` with exactly one argument and no flags. The
# daemon itself never parses NOTA (its package is built without `nota-text`).
#
# THE NOTA → rkyv ENCODE IS A DEPLOY STEP, NOT A FLAG. `criome-daemon` accepts
# only a pre-generated rkyv `SignalFile` as its one argument and rejects inline
# NOTA and `.nota` paths. So the typed `CriomeDaemonConfiguration` — socket
# path, store path, optional cluster-root trust anchor — is encoded here, in
# `ExecStartPre`, by the one-argument `criome-encode-configuration` deploy
# encoder (which lives in the `nota-text` encoder package, not the daemon one).
#
# KEY CUSTODY (Spirit `psc6` / key-custody `q1le`). criome's master signing key
# is generated on first run and persisted to a `0600` file by the daemon's own
# `MasterKey::load_or_generate`. The daemon derives that path from the store
# path: store `…/criome.sema` ⇒ key `…/criome.masterkey`. This module does not
# write or read the key; it only provisions the owning state directory (mode
# `0700`) so the daemon can create the key file there. The secret never leaves
# criome.
#
# SELF-RESUME ON RESTART. The daemon's durable state is the SEMA store under the
# persisted `StateDirectory`. On a restart the daemon re-opens that store and
# re-reads the master key file, resuming from persisted state — the
# `ExecStartPre` re-encode of the (deterministic) configuration is idempotent
# and never touches the store or the key.
#
# `clusterRootPublicKey` is left unset by default, which the encoder lowers to
# `cluster_root: None` — a virgin daemon that starts unconfigured for the trust
# anchor and waits for the eventual authenticated meta-signal key configuration.

{
  config,
  lib,
  pkgs,
  ...
}:

let
  cfg = config.services.criome;

  # The runtime layout under the service's state/runtime dirs. The store and the
  # daemon-derived master key live under the PERSISTED state dir (so SEMA and the
  # key survive a restart); the regenerated config rkyv lives under the volatile
  # runtime dir.
  runtimeDir = "/run/criome";
  stateDir = "/var/lib/criome";
  socketPath = "${runtimeDir}/${cfg.socketName}";
  storePath = "${stateDir}/criome.sema";
  configRkyv = "${runtimeDir}/criome-config.rkyv";

  clusterRootField =
    if cfg.clusterRootPublicKey == null then "None" else "(Some ${cfg.clusterRootPublicKey})";

  # The single typed `CriomeDaemonConfiguration` record, positional NOTA: the
  # three fields in declared order (socket_path, store_path, cluster_root) with
  # NO type-name head — the schema-derived `NotaDecode` reads the parenthesised
  # body as the field vector. The encoder wraps it in a
  # `CriomeConfigurationArtifact` carrying the output path and seals it to rkyv.
  configurationNota =
    "(CriomeConfigurationArtifact "
    + "(${socketPath} ${storePath} ${clusterRootField}) "
    + "${configRkyv})";

  encodeConfigurationScript = pkgs.writeShellScript "criome-encode-configuration" ''
    set -eu
    ${cfg.encoderPackage}/bin/criome-encode-configuration ${lib.escapeShellArg configurationNota}
  '';
in
{
  options.services.criome = {
    enable = lib.mkEnableOption "the criome BLS-attestation daemon";

    daemonPackage = lib.mkOption {
      type = lib.types.package;
      description = "Package providing the `criome-daemon` binary (built without nota-text).";
    };

    encoderPackage = lib.mkOption {
      type = lib.types.package;
      description = ''
        Package providing the deploy encoder `criome-encode-configuration`
        (built with the nota-text feature). The daemon package must NOT carry
        nota-text; this one does the typed NOTA → rkyv encode at deploy time.
      '';
    };

    socketName = lib.mkOption {
      type = lib.types.str;
      default = "criome.sock";
      description = ''
        The working Unix socket file name under ${runtimeDir}. The daemon binds
        this socket at 0600 itself; clients (e.g. a co-located message-router)
        point their `criome_socket_path` at it.
      '';
    };

    clusterRootPublicKey = lib.mkOption {
      type = lib.types.nullOr lib.types.str;
      default = null;
      example = "b1c2…";
      description = ''
        The cluster-root BLS public key (hex), the trust anchor whose signature
        admits keys into the registry. A bare NOTA atom. Left null starts the
        daemon without a configured anchor (virgin bootstrap), awaiting the
        authenticated meta-signal key configuration.
      '';
    };
  };

  config = lib.mkIf cfg.enable {
    systemd.services.criome = {
      description = "criome BLS-attestation daemon";
      wantedBy = [ "multi-user.target" ];
      after = [ "network.target" ];
      serviceConfig = {
        Type = "simple";
        Restart = "on-failure";
        # The runtime dir holds the regenerated config rkyv + the bound socket;
        # the state dir (0700) holds the durable SEMA store and the daemon's
        # 0600 master key, and is PRESERVED across restarts so the daemon
        # self-resumes from persisted state.
        RuntimeDirectory = "criome";
        StateDirectory = "criome";
        StateDirectoryMode = "0700";
        ExecStartPre = [ encodeConfigurationScript ];
        ExecStart = "${cfg.daemonPackage}/bin/criome-daemon ${configRkyv}";
      };
    };
  };
}
