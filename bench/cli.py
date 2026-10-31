"""CLI бенчмарка: python -m bench {run,compare,preflight,cleanup,selftest}.

Команды:
  run        — прогон ramp'а против стенда (prefflight → нагрузка → артефакты)
  compare    — отчёт «до → после» по двум result.json
  preflight  — проверки стенда без прогона
  cleanup    — удалить артефакты бенчмарка (стримы с префиксом bench-cam-)
  selftest   — полный прогон против встроенного мока с известной ёмкостью
"""
from __future__ import annotations

import argparse
import copy
import io
import logging
import sys
from pathlib import Path

from bench.config import PROFILES, RESULTS_DIR, RunConfig

logger = logging.getLogger(__name__)


def main(argv: list[str] | None = None) -> int:
    # Консоль может быть cp1251 (Windows без PYTHONIOENCODING): кириллица
    # оттуда пройдёт, а ✅/❌ — нет. Вместо краха после двух минут selftest
    # недопечатываемые глифы заменяем, а не роняем прогон.
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            try:
                stream.reconfigure(errors="replace")
            except (io.UnsupportedOperation, ValueError):
                pass
    parser = _build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    if args.command == "run":
        return _cmd_run(args)
    if args.command == "compare":
        return _cmd_compare(args)
    if args.command == "preflight":
        return _cmd_preflight(args)
    if args.command == "cleanup":
        return _cmd_cleanup(args)
    if args.command == "selftest":
        from bench.selftest import run_selftest

        return run_selftest()
    parser.print_help()
    return 2


# ============================================================================
# Парсер аргументов
# ============================================================================
def _add_endpoint_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--api-url", default=None, help="API стенда (или BENCH_API_URL)")
    p.add_argument("--prom-url", default=None, help="Prometheus (или BENCH_PROM_URL)")
    p.add_argument("--rtsp-publish", default=None,
                   help="куда ffmpeg пушит RTSP (или BENCH_RTSP_PUBLISH)")
    p.add_argument("--rtsp-source", default=None,
                   help="что читает producer в docker-сети (или BENCH_RTSP_SOURCE)")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="bench",
        description=(
            "Внешний автобенчмарк Vision Analytics (стримы): строго через "
            "HTTP API (:8000) и Prometheus (:9090), код приложения не модифицируется."
        ),
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="отладочный лог")
    sub = parser.add_subparsers(dest="command", required=True)

    # -------------------------------------------------------------------- run
    p_run = sub.add_parser("run", help="прогнать бенчмарк (ramp → окна → drain)")
    p_run.add_argument("--profile", default="default", choices=list(PROFILES),
                       help="набор ступеней/окон: quick | default | thorough")
    p_run.add_argument("--label", default="", help="метка прогона (напр. 'replicas=4')")
    p_run.add_argument("--notes", default="",
                       help="что менял в системе — попадёт в отчёт и в compare")
    p_run.add_argument("--video", default="",
                       help="ролик-источник (нужен без --mock; один и тот же для "
                            "сравнимости прогонов)")
    p_run.add_argument("--mock", action="store_true",
                       help="синтетический стенд вместо реального (selftest)")
    _add_endpoint_args(p_run)
    p_run.add_argument("--scrape-interval", type=float, default=None,
                       help="интервал скрейпа Prometheus стенда, с (дефолт 15)")
    p_run.add_argument("--keep-artifacts", action="store_true",
                       help="не удалять созданные стримы после прогона")
    p_run.add_argument("--no-drain", action="store_true",
                       help="не замерять фазу разгребания бэклога")
    p_run.add_argument("--skip-preflight", action="store_true",
                       help="запускать даже если preflight красный")
    p_run.add_argument("--out-dir", default=None, help="куда складывать артефакты")
    # --- переопределения ступеней ---
    p_run.add_argument("--scale", choices=["streams", "fps"], default=None,
                       help="масштаб ступеней (число камер или fps одного)")
    p_run.add_argument("--streams", default=None,
                       help="ступени числа камер, напр. '2,4,6,8'")
    p_run.add_argument("--fps", type=int, default=None,
                       help="fps каждого стрима (напр. 5)")
    p_run.add_argument("--settle", type=float, default=None,
                       help="прогрев ступени, с")
    p_run.add_argument("--measure", type=float, default=None,
                       help="окно измерения, с")
    p_run.add_argument("--drain-timeout", type=float, default=None,
                       help="таймаут фазы drain, с")

    # ---------------------------------------------------------------- compare
    p_cmp = sub.add_parser("compare", help="сравнить два прогона (до → после)")
    p_cmp.add_argument("before", help="result.json или каталог прогона «до»")
    p_cmp.add_argument("after", help="result.json или каталог прогона «после»")
    p_cmp.add_argument("--out", default=None, help="куда писать отчёт (.md)")

    # ------------------------------------------------------------- preflight
    p_pre = sub.add_parser("preflight", help="проверить стенд без прогона")
    p_pre.add_argument("--video", default="", help="ролик-источник для проверки")
    p_pre.add_argument("--mock", action="store_true")
    _add_endpoint_args(p_pre)

    # ---------------------------------------------------------------- cleanup
    p_clean = sub.add_parser("cleanup", help="удалить артефакты бенчмарка из API")
    _add_endpoint_args(p_clean)

    # ---------------------------------------------------------------- selftest
    sub.add_parser("selftest",
                   help="прогон против встроенного мока с известной ёмкостью")
    return parser


# ============================================================================
# Команды
# ============================================================================
def _apply_endpoints(cfg: RunConfig, args: argparse.Namespace) -> None:
    if getattr(args, "api_url", None):
        cfg.endpoints.api_url = args.api_url.rstrip("/")
    if getattr(args, "prom_url", None):
        cfg.endpoints.prometheus_url = args.prom_url.rstrip("/")
    if getattr(args, "rtsp_publish", None):
        cfg.endpoints.rtsp_publish_url = args.rtsp_publish
    if getattr(args, "rtsp_source", None):
        cfg.endpoints.rtsp_source_url = args.rtsp_source


def _build_config(args: argparse.Namespace) -> RunConfig:
    profile = PROFILES.get(args.profile, PROFILES["default"])
    cfg = RunConfig(
        profile_name=args.profile,
        label=args.label,
        notes=args.notes,
        source_video=args.video,
        mock=args.mock,
        keep_artifacts=args.keep_artifacts,
        measure_drain=not args.no_drain,
        scrape_interval_s=args.scrape_interval or 15.0,
    )
    # Профили — shared-инстансы: deepcopy, чтобы переопределения не протекли
    # в PROFILES и в следующие прогоны.
    cfg.stream = copy.deepcopy(profile["stream"])
    _apply_endpoints(cfg, args)
    if args.out_dir:
        cfg.results_dir = Path(args.out_dir)

    if args.scale:
        cfg.stream.scale = args.scale
    if args.streams:
        cfg.stream.steps = [int(x) for x in args.streams.split(",") if x.strip()]
    if args.fps:
        cfg.stream.fps = args.fps
    if args.settle is not None:
        cfg.stream.settle_s = args.settle
    if args.measure is not None:
        cfg.stream.measure_s = args.measure
    if args.drain_timeout:
        cfg.stream.drain_timeout_s = args.drain_timeout
    return cfg


def _cmd_run(args: argparse.Namespace) -> int:
    cfg = _build_config(args)
    if not cfg.mock and not args.skip_preflight:
        if _cmd_preflight(args) != 0:
            print("\nPreflight провалился — прогон отменён "
                  "(запустить всё равно: --skip-preflight)")
            return 2

    from bench.reports import persist_run
    from bench.runner import run_benchmark

    result = run_benchmark(cfg)
    out_dir = cfg.results_dir / result.run_id
    paths = persist_run(result, out_dir)
    _print_summary(result, paths)
    return 1 if result.summary.get("aborted") else 0


def _cmd_compare(args: argparse.Namespace) -> int:
    from bench.compare import compare_runs, load_result, plot_compare

    before = load_result(args.before)
    after = load_result(args.after)
    md = compare_runs(before, after)
    out = Path(args.out) if args.out else RESULTS_DIR / (
        f"compare_{before.get('run_id', 'a')}_vs_{after.get('run_id', 'b')}.md"
    )
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(md, encoding="utf-8")
    try:
        chart = out.with_suffix(".png")
        plot_compare(before, after, chart)
        print(f"График сравнения: {chart}")
    except Exception as exc:  # noqa: BLE001 — отчёт важнее графика
        logger.warning("График сравнения не построен: %s", exc)
    print(md)
    print(f"\nОтчёт: {out}")
    return 0


def _cmd_preflight(args: argparse.Namespace) -> int:
    from bench.preflight import run_preflight

    cfg = RunConfig(profile_name="preflight")
    cfg.source_video = getattr(args, "video", "") or ""
    cfg.mock = bool(getattr(args, "mock", False))
    _apply_endpoints(cfg, args)
    checks = run_preflight(cfg)
    print("\nPreflight стенда:")
    failed = 0
    for name, ok, detail in checks:
        print(f"  {'✅' if ok else '❌'} {name}: {detail}")
        failed += 0 if ok else 1
    print(f"{'Стенд готов к прогону' if not failed else 'Стенд НЕ готов'}\n")
    return 0 if not failed else 1


def _cmd_cleanup(args: argparse.Namespace) -> int:
    from bench.api_client import ApiError, VisionApiClient

    cfg = RunConfig(profile_name="cleanup")
    _apply_endpoints(cfg, args)
    api = VisionApiClient(cfg.endpoints.api_url)
    removed = 0
    try:
        for s in api.list_streams():
            if str(s.get("name", "")).startswith("bench-cam-"):
                api.delete_stream(s["id"])
                removed += 1
    except ApiError as exc:
        print(f"Ошибка очистки: {exc}")
        return 1
    print(f"Удалено артефактов бенчмарка: {removed}")
    return 0


def _print_summary(result, paths: dict) -> None:
    s = result.summary
    ms = s.get("max_sustainable") or {}
    cp = s.get("collapse_point") or {}
    bn = s.get("bottleneck") or {}
    print("\n" + "=" * 66)
    print(f"ИТОГ · конвейер «{result.pipeline}» · {result.run_id}"
          + (f" · label «{result.label}»" if result.label else ""))
    if s.get("aborted"):
        print(f"⚠️  Прогон прерван: {s['aborted']} (результаты частичные)")
    if ms.get("offered") is not None:
        print(f"  Устойчивый максимум: {ms['offered']:g} fps (шаг {ms.get('step')})")
    if cp.get("offered") is not None:
        print(f"  Точка коллапса:      {cp['offered']:g} fps (шаг {cp.get('step')})")
    else:
        print("  Система не насыщена в проверенном диапазоне ступеней")
    print(f"  Узкое место: {bn.get('stage')}")
    for ev in (bn.get("evidence") or [])[:3]:
        print(f"    • {ev}")
    if result.drain:
        print(f"  Drain: {result.drain.get('duration_s', 0):.0f}с")
    print("  Артефакты:")
    for key in ("report_md", "chart", "result_json"):
        p = paths.get(key)
        if p:
            print(f"    {key:11s} {p}")
    print("=" * 66)


if __name__ == "__main__":
    sys.exit(main())
