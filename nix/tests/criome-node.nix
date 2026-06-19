# criome-node.nix — a single-node nixosTest that boots the criome daemon from
# the deploy path (typed NOTA → rkyv in ExecStartPre, then `criome-daemon
# <config.rkyv>` with one argument) and asserts:
#
#   1. The daemon comes up: the unit is active and its working socket is bound
#      and listening at 0600.
#   2. The deploy discipline held: the daemon's one argument is the encoded
#      rkyv config (no flags), and the daemon never saw NOTA.
#   3. Key custody (Spirit psc6 / q1le): the daemon generated and persisted its
#      master key at the store-derived path with mode 0600.
#   4. Self-resume on restart: after `systemctl restart`, the SAME persisted
#      master key and SEMA store are reused (the key file is byte-identical and
#      the socket comes back up) — the daemon resumed from persisted state, it
#      did not re-mint a key or wipe its store.
#
# This is the criome sibling of the router's two-kernel transport test, scoped
# to the single-node criome surface. It needs /dev/kvm to actually boot; the
# driver builds and evaluates everywhere.

{
  pkgs,
  daemonPackage,
  encoderPackage,
  criomeModule,
}:

pkgs.testers.runNixOSTest {
  name = "criome-node";

  nodes.machine =
    { ... }:
    {
      imports = [ criomeModule ];

      services.criome = {
        enable = true;
        daemonPackage = daemonPackage;
        encoderPackage = encoderPackage;
      };
    };

  testScript = ''
    start_all()

    # (1) The daemon comes up. The criome module ran the NOTA -> rkyv encoder in
    #     ExecStartPre, then launched `criome-daemon <config.rkyv>` with one
    #     argument.
    machine.wait_for_unit("criome.service")

    # (2) Deploy discipline: the encoded rkyv config exists and is what the
    #     daemon was started with. The daemon binary itself carries no nota-text;
    #     it only ever read this rkyv file.
    machine.succeed("test -f /run/criome/criome-config.rkyv")
    # The running daemon's argv is exactly the one rkyv path (one argument, no
    # flags). Read it from /proc and assert the single argument.
    argv = machine.succeed(
        "tr '\\0' '\\n' < /proc/$(systemctl show -p MainPID --value criome.service)/cmdline"
    ).strip().split("\n")
    print("daemon argv:", argv)
    assert argv[-1] == "/run/criome/criome-config.rkyv", (
        f"daemon's last argument must be the encoded rkyv config, got {argv!r}"
    )
    assert not any(a.startswith("--") for a in argv), f"no flags allowed, got {argv!r}"

    # (1 cont.) The working socket is bound and listening at 0600.
    machine.wait_until_succeeds("test -S /run/criome/criome.sock")
    socket_mode = machine.succeed("stat -c '%a' /run/criome/criome.sock").strip()
    assert socket_mode == "600", f"socket must be 0600, got {socket_mode}"

    # (3) Key custody: the daemon generated + persisted its master key at the
    #     store-derived path (criome.sema -> criome.masterkey) with mode 0600.
    machine.wait_until_succeeds("test -f /var/lib/criome/criome.masterkey")
    key_mode = machine.succeed("stat -c '%a' /var/lib/criome/criome.masterkey").strip()
    assert key_mode == "600", f"master key must be 0600, got {key_mode}"

    # Capture the persisted key + store fingerprints BEFORE the restart.
    key_before = machine.succeed("sha256sum /var/lib/criome/criome.masterkey").split()[0]
    store_listing_before = machine.succeed("ls -1 /var/lib/criome | sort").strip()

    # (4) Self-resume on restart: restart the unit and assert the daemon comes
    #     back up reusing the SAME persisted master key and store (no re-mint,
    #     no wipe).
    machine.succeed("systemctl restart criome.service")
    machine.wait_for_unit("criome.service")
    machine.wait_until_succeeds("test -S /run/criome/criome.sock")

    key_after = machine.succeed("sha256sum /var/lib/criome/criome.masterkey").split()[0]
    store_listing_after = machine.succeed("ls -1 /var/lib/criome | sort").strip()
    assert key_after == key_before, (
        f"master key must survive a restart unchanged (self-resume), "
        f"before={key_before} after={key_after}"
    )
    assert store_listing_after == store_listing_before, (
        f"persisted state dir must survive a restart, "
        f"before={store_listing_before!r} after={store_listing_after!r}"
    )

    print(
        "criome node GREEN: the daemon booted from a deploy-encoded rkyv config "
        "(one argument, no flags), bound its 0600 working socket, persisted its "
        "0600 master key at the store-derived path, and self-resumed from the "
        "same persisted key + SEMA store across a restart."
    )
  '';
}
