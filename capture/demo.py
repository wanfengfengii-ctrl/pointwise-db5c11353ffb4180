"""Combined delivery demo: V1 capture ledgers plus late configuration binding."""
from pathlib import Path
from capture.service import main as service_main
from capture.version_fixture import main as fixture_main

ROOT = Path(__file__).resolve().parents[1]


def main(argv=None):
    service_main([
        str(ROOT / 'samples/day_capture.json'),
        str(ROOT / 'samples/gap_repair.json'),
        str(ROOT / 'samples/restart_audit.json'),
        '--expected', str(ROOT / 'samples/expected_results.json'),
    ])
    fixture_main(['--check-port'])
    fixture_main(['--case', str(ROOT / 'samples/config_versions.json')])
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
