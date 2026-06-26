from __future__ import annotations

from tools import live_host_readiness_check as check


def test_inactive_watchdog_reports_restart_action(monkeypatch) -> None:
    def fake_which(name: str) -> str:
        return f"/usr/bin/{name}"

    def fake_run(args: list[str]) -> check.CommandResult:
        if args[:2] == ["swapon", "--show=SIZE"]:
            return check.CommandResult(0, f"{check.MIN_SWAP_BYTES}\n", "")
        unit = args[2]
        if unit == "trading-engine.service":
            return check.CommandResult(
                0,
                "\n".join(
                    [
                        "LoadState=loaded",
                        "ActiveState=inactive",
                        "Type=notify",
                        "WatchdogUSec=infinity",
                        f"MemoryMax={32 * 1024 * 1024 * 1024}",
                        "OOMScoreAdjust=600",
                    ]
                ),
                "",
            )
        return check.CommandResult(
            0,
            "\n".join(
                [
                    "LoadState=loaded",
                    "ActiveState=active",
                    "Type=notify",
                    "WatchdogUSec=1min",
                    f"MemoryMax={6 * 1024 * 1024 * 1024}",
                    "OOMScoreAdjust=700",
                ]
            ),
            "",
        )

    monkeypatch.setattr(check.shutil, "which", fake_which)
    monkeypatch.setattr(check, "_run", fake_run)

    errors = check.live_host_readiness_errors()

    assert errors == [
        "trading-engine.service:watchdog_not_applied:WatchdogUSec=infinity:"
        "ActiveState=inactive:run sudo systemctl daemon-reload && "
        "sudo systemctl restart trading-engine.service"
    ]


def test_absent_systemd_units_do_not_block_compose_live_validation(monkeypatch) -> None:
    def fake_which(name: str) -> str:
        return f"/usr/bin/{name}"

    def fake_run(args: list[str]) -> check.CommandResult:
        if args[:2] == ["swapon", "--show=SIZE"]:
            return check.CommandResult(0, f"{check.MIN_SWAP_BYTES}\n", "")
        return check.CommandResult(0, "LoadState=not-found\n", "")

    monkeypatch.setattr(check.shutil, "which", fake_which)
    monkeypatch.setattr(check, "_run", fake_run)

    assert check.live_host_readiness_errors() == []
