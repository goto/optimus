"""Tests for SuperExternal3rdPartyTaskSensor poke logic in __lib.py.

Airflow is stubbed out, so this runs with only the standard library:

    python3 -m unittest ext/scheduler/airflow/test_lib_third_party_sensor.py

Regression context: `_hard_check` once had its (schedule_time, is_available) arguments swapped
at the call sites, which made every poke return True and silently disabled the dex sensor.
"""
import importlib.util
import logging
import os
import sys
import types
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock


class _Stub:
    """Generic stand-in usable as a base class, a callable, or a context-free object."""

    def __init__(self, *args, **kwargs):
        pass


class _StubModule(types.ModuleType):
    def __getattr__(self, name):
        if name.startswith("__"):
            raise AttributeError(name)
        stub = type(name, (_Stub,), {})
        setattr(self, name, stub)
        return stub


class _Variable:
    values = {}

    @classmethod
    def get(cls, key, default_var=None):
        return cls.values.get(key, default_var)


_STUBBED_MODULES = [
    "pendulum",
    "requests",
    "requests.adapters",
    "urllib3",
    "urllib3.util",
    "urllib3.util.retry",
    "airflow",
    "airflow.configuration",
    "airflow.hooks",
    "airflow.hooks.base",
    "airflow.models",
    "airflow.utils",
    "airflow.utils.xcom",
    "airflow.utils.state",
    "airflow.providers",
    "airflow.providers.cncf",
    "airflow.providers.cncf.kubernetes",
    "airflow.providers.cncf.kubernetes.operators",
    "airflow.providers.cncf.kubernetes.operators.kubernetes_pod",
    "airflow.providers.cncf.kubernetes.operators.pod",
    "airflow.providers.slack",
    "airflow.providers.slack.operators",
    "airflow.providers.slack.operators.slack",
    "airflow.sensors",
    "airflow.sensors.base",
    "airflow.exceptions",
    "croniter",
    "kubernetes",
    "kubernetes.client",
]


def _load_lib():
    saved = {name: sys.modules.get(name) for name in _STUBBED_MODULES}
    try:
        for name in _STUBBED_MODULES:
            sys.modules[name] = _StubModule(name)
        sys.modules["airflow.models"].Variable = _Variable
        sys.modules["airflow.models"].XCOM_RETURN_KEY = "return_value"

        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "__lib.py")
        spec = importlib.util.spec_from_file_location("optimus_airflow_lib_under_test", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        for name, original in saved.items():
            if original is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = original


lib = _load_lib()

SCHEDULE_TIME = datetime(2026, 7, 8, 0, 0, tzinfo=timezone.utc)


def _make_sensor():
    sensor = lib.SuperExternal3rdPartyTaskSensor.__new__(lib.SuperExternal3rdPartyTaskSensor)
    sensor.project_name = "proj"
    sensor.job_name = "job"
    sensor.third_party_type = "dex"
    sensor.identifier = "maxcompute://p.s.t"
    sensor.config = {}
    sensor._third_party_types_supported = ["dex"]
    sensor.log = logging.getLogger("test_sensor")
    return sensor


def _context(run_id="scheduled__2026-07-08", start_date=None):
    dag_run = mock.Mock()
    dag_run.run_id = run_id
    dag_run.start_date = start_date or datetime.now(timezone.utc)
    return {"dag_run": dag_run}


class SensorPokeTest(unittest.TestCase):
    def setUp(self):
        _Variable.values = {}
        patcher = mock.patch.object(lib, "get_scheduled_at", return_value=SCHEDULE_TIME)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.sensor = _make_sensor()

    def _poke(self, mode, available, is_5xx=False, context=None, **variables):
        _Variable.values = {lib.THIRD_PARTY_SENSOR_TOGGLE: mode}
        _Variable.values.update(variables)
        with mock.patch.object(
            lib.SuperExternal3rdPartyTaskSensor,
            "is_upstream_data_available",
            return_value=(available, is_5xx),
        ):
            return self.sensor.poke(context or _context())

    # ON: hard failure

    def test_on_reschedules_when_data_incomplete(self):
        self.assertFalse(self._poke(lib.THIRD_PARTY_SENSOR_TOGGLE_ON, available=False))

    def test_on_succeeds_when_data_complete(self):
        self.assertTrue(self._poke(lib.THIRD_PARTY_SENSOR_TOGGLE_ON, available=True))

    # SOFT: hard failure until the max wait elapses

    def test_soft_reschedules_within_max_wait_when_incomplete(self):
        result = self._poke(
            lib.THIRD_PARTY_SENSOR_TOGGLE_SOFT, available=False,
            **{lib.THIRD_PARTY_SENSOR_MAX_TIME: "60"})
        self.assertFalse(result)

    def test_soft_succeeds_within_max_wait_when_complete(self):
        result = self._poke(
            lib.THIRD_PARTY_SENSOR_TOGGLE_SOFT, available=True,
            **{lib.THIRD_PARTY_SENSOR_MAX_TIME: "60"})
        self.assertTrue(result)

    def test_soft_bypasses_after_max_wait_even_when_incomplete(self):
        ctx = _context(start_date=datetime.now(timezone.utc) - timedelta(minutes=120))
        result = self._poke(
            lib.THIRD_PARTY_SENSOR_TOGGLE_SOFT, available=False, context=ctx,
            **{lib.THIRD_PARTY_SENSOR_MAX_TIME: "60"})
        self.assertTrue(result)

    def test_soft_without_max_wait_always_succeeds(self):
        self.assertTrue(self._poke(lib.THIRD_PARTY_SENSOR_TOGGLE_SOFT, available=False))

    # SOFT_5XX: hard unless dex returned a 5xx

    def test_soft_5xx_reschedules_when_incomplete_without_server_error(self):
        result = self._poke(
            lib.THIRD_PARTY_SENSOR_TOGGLE_SOFT_5XX, available=False, is_5xx=False,
            **{lib.THIRD_PARTY_SENSOR_MAX_TIME: "60"})
        self.assertFalse(result)

    def test_soft_5xx_reschedules_on_server_error_within_max_wait(self):
        result = self._poke(
            lib.THIRD_PARTY_SENSOR_TOGGLE_SOFT_5XX, available=False, is_5xx=True,
            **{lib.THIRD_PARTY_SENSOR_MAX_TIME: "60"})
        self.assertFalse(result)

    def test_soft_5xx_bypasses_on_server_error_after_max_wait(self):
        ctx = _context(start_date=datetime.now(timezone.utc) - timedelta(minutes=120))
        result = self._poke(
            lib.THIRD_PARTY_SENSOR_TOGGLE_SOFT_5XX, available=False, is_5xx=True, context=ctx,
            **{lib.THIRD_PARTY_SENSOR_MAX_TIME: "60"})
        self.assertTrue(result)

    def test_soft_5xx_does_not_bypass_incomplete_after_max_wait_without_server_error(self):
        ctx = _context(start_date=datetime.now(timezone.utc) - timedelta(minutes=120))
        result = self._poke(
            lib.THIRD_PARTY_SENSOR_TOGGLE_SOFT_5XX, available=False, is_5xx=False, context=ctx,
            **{lib.THIRD_PARTY_SENSOR_MAX_TIME: "60"})
        self.assertFalse(result)

    # OFF / bypasses

    def test_off_skips_check(self):
        self.assertTrue(self._poke(lib.THIRD_PARTY_SENSOR_TOGGLE_OFF, available=False))

    def test_custom_backfill_run_is_bypassed(self):
        result = self._poke(
            lib.THIRD_PARTY_SENSOR_TOGGLE_ON, available=False,
            context=_context(run_id="custom-backfill_2026-07-08"))
        self.assertTrue(result)


if __name__ == "__main__":
    unittest.main()
