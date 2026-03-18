import asyncio
import datetime
import json
import random
import string
import subprocess
from collections import defaultdict
from collections.abc import Iterable
from copy import deepcopy
from itertools import chain
from typing import Dict, Any, List
from pprint import pformat

try:
    from benchmark import (
        start_benchmark,
        end_benchmark,
        benchmark_decorator)
    benchmark_loaded = True
except:
    benchmark_loaded = False

from filesystem import (
    CONFIG_FOLDER,
    get_config_folder,
    file_exists,
    get_file_modification_time,
    create_yaml,
    load_yaml,
    save_yaml)
from hass_manager import (
    get_state,
    get_attr,
    set_state,
    set_attr,
    get_manufacturer,
    get_identifiers,
    get_integration,
    reload_integration)
from history import (
    interpolate_data,
    get_values,
    get_average_value,
    get_max_value,
    get_min_value)
from i18n import (
    I18nCatalog)
from mynotify import (
    my_notify,
    my_persistent_notification)
from mytime import (
    datetime_to_unix,
    getTime,
    getTimePlusDays,
    monthsBetween,
    daysBetween,
    hoursBetween,
    minutesBetween,
    getMinute,
    getHour,
    getMonth,
    getYear,
    getMonthFirstDay,
    getTimeStartOfDay,
    getTimeEndOfDay,
    getDayOfWeek,
    getDayOfWeekText,
    date_to_string,
    toDateTime,
    resetDatetime,
    reset_time_to_hour,
    is_day,
    add_months)
from utils import (
    in_between,
    round_up,
    average,
    calculate_ema,
    calculate_trend,
    reverse_list,
    get_specific_values,
    get_closest_key,
    get_dict_value_with_path,
    delete_dict_key_with_path,
    dicts_equal,
    rename_dict_keys,
    compare_dicts_unique_to_dict1,
    update_dict_with_new_keys,
    limit_dict_size,
    contains_any,
    flatten_dict,
    delete_flattened_key,
    check_next_24_hours_diff,
    time_window_minutes_left,
    time_window_minutes_left_from_datetime,
    time_window_linear_weight,
    time_window_parabolic_weight,
    time_window_gaussian_weight)

import homeassistant.helpers.sun as sun

from logging import getLogger
TITLE = f"BattMind ({__name__}.py)"
BASENAME = f"pyscript.{__name__}"
_LOGGER = getLogger(BASENAME)

INITIALIZATION_COMPLETE = False
TESTING = False
SOLAR_CONFIGURED = None
POWERWALL_CONFIGURED = None

LAST_SUCCESSFUL_GRID_PRICES = {
    "using_offline_prices": False
}

CHARGING_IS_BEGINNING = False
RESTARTING_CHARGER = False
CURRENT_CHARGING_AMPS = [0, 0, 0]

ERROR_COUNT = 0

ENTITY_UNAVAILABLE_STATES = (None, "unavailable", "unknown")
POWERWALL_ACTION_STATES = ("grid_charging", "discharge_allowed", "stopped", "force_discharge", "force_charge")

FORECAST_TYPE = "ema"

INSTANCE_ID = random.randint(0, 10000)
TASKS = {}

CONFIG = {}
CONFIG_LAST_MODIFIED = None

KWH_AVG_PRICES_DB = {}
KWH_AVG_PRICES_DB_VERSION = 1.0

POWER_VALUES_DB = {}
POWER_VALUES_DB_VERSION = 1.0

SOLAR_PRODUCTION_AVAILABLE_DB = {}
SOLAR_PRODUCTION_AVAILABLE_DB_VERSION = 2.0

CHARGING_HISTORY_ENDING_BYTE_SIZE = None
CHARGING_HISTORY_DB = {}
OVERVIEW_HISTORY = {}

BATTERY_LEVEL_EXPENSES = {}
CHARGING_PLAN = {}
CHARGE_HOURS = {}

LOCAL_ENERGY_PRICES = {
    "solar_kwh_price": {},
    "powerwall_kwh_price": {},
}

LOCAL_ENERGY_PREDICTION_DB = {
    "solar_prediction": {},
    "solar_prediction_timestamps": {},
}

SOLAR_SELL_TARIFF = {
    "energinets_network_tariff": 0.0030,
    "energinets_balance_tariff": 0.0024,
    "solar_production_seller_cut": 0.01
}

INTEGRATION_OFFLINE_TIMESTAMP = {}

WEATHER_CONDITION_DICT = {
    "sunny": 100,                # Maximum solar production; ideal conditions
    "windy": 80,                 # Minimal impact on solar production
    "windy-variant": 80,         # Similar to 'windy'; minor impact on production
    "partlycloudy": 60,          # Moderately reduced solar production
    "cloudy": 40,                # Significantly reduced solar production
    "rainy": 40,                 # Significantly reduced production due to rain and clouds
    "pouring": 20,               # Heavy rain; large reduction in production
    "lightning": 20,             # Large reduction due to cloud cover and storm
    "lightning-rainy": 20,       # Severe storm and rain; significantly reduced production
    "snowy": 20,                 # Snow; markedly reduces solar production
    "snowy-rainy": 20,           # Combination of snow and rain; very low production
    "clear-night": 0,            # No production at night
    "fog": 20,                   # Fog; very low production due to reduced sunlight
    "hail": 0,                   # No production during hail
    "exceptional": 0             # Extreme conditions; no production
}

CHARGING_TYPES = {
    "error": {
        "priority": 1,
        "emoji": "☠️",
    },
    "no_rule": {
        "priority": 2,
        "emoji": "⚠️",
    },
    "cheapest_hour_fill_planner": {
        "priority": 3,
        "emoji": "💸",
    },
    "most_expensive_planner": {
        "priority": 3.1,
        "emoji": "🔥",
    },
    "charging": {
        "priority": 4,
        "emoji": "📈",
    },
    "discharging": {
        "priority": 4.1,
        "emoji": "📉",
    },
    "blocked": {
        "priority": 4.2,
        "emoji": "❌",
    },
    "solar": {
        "priority": 6,
        "emoji": "☀️",
    },
    "solar_corrected": {
        "priority": 6.1,
        "emoji": "🌤️",
    },
    "balance": {
        "priority": 7,
        "emoji": "⚖️",
    },
    "sunrise": {
        "priority": 96,
        "emoji": "🌅",
    },
    "sunset": {
        "priority": 97,
        "emoji": "🌇",
    },
    "profit": {
        "priority": 100,
        "emoji": "💰",
    },
    "average": {
        "priority": 999,
        "emoji": "⌛",
    }
}

DEFAULT_CONFIG = {
    "cron_interval": 5,
    "database": {
        "power_values_db_data_to_save": 15,
        "solar_available_db_data_to_save": 10,
        "charging_history_db_data_to_save": 12
    },
    "first_run": True,
    "forecast": {
        "entity_ids": {
            "hourly_service_entity_id": "",
            "daily_service_entity_id": "",
        }
    },
    "home": {
        "entity_ids": {
            "power_consumption_entity_id": "",
            "powerwall_watt_flow_entity_id": "",
            "powerwall_battery_level_entity_id": "",
            "ignore_consumption_from_entity_ids": []
        },
        "power_consumption_entity_id_include_powerwall_charging": False,
        "power_consumption_entity_id_include_powerwall_discharging": False,
        "invert_powerwall_watt_flow_entity_id": False
    },
    "language": "da-DK", #Set default language to en-GB in later update
    "notification": {
        "update_available": True,
    },
    "notify_list": [],
    "prices": {
        "entity_ids": {
            "power_prices_entity_id": ""
        },
        "refund": 0.0,
        "cheap_price_periods": 2,
        "cheap_price_period_rise_threshold": 0.10,
        "cheapest_price_rise_threshold": 0.50,
    },
    "solar": {
        "entity_ids": {
            "production_entity_id": "",
            "forecast_entity_id": ""
        },
        "solarpower_use_before_minutes": 60,
        "production_price": -1.00,
        "powerwall_battery_size": 10.0,
        "inverter_discharging_power_limit": 5000.0,
        "powerwall_charging_power_limit": 5000.0,
        "powerwall_discharging_power": 5000.0,
        "powerwall_battery_level_min": 5.0,
        "powerwall_battery_level_max": 100.0,
        "powerwall_charge_discharge_loss": 0.1,
        "powerwall_wear_cost_per_kwh": 0.1,
    },
    "testing_mode": False
}

CONFIG_KEYS_RENAMING = {# Old path: New path (seperated by ".")
}

COMMENT_DB_YAML = {}

DEFAULT_ENTITIES = {
    "homeassistant": {
        "customize": {
            f"input_select.battmind_select_release": {},
            f"input_button.{__name__}_enforce_planning": {},
            f"input_button.{__name__}_restart_script": {},
            f"input_boolean.{__name__}_debug_log": {},
            f"input_boolean.{__name__}_deactivate_script": {},
            f"input_number.{__name__}_kwh_charged_by_solar": {},
            f"input_number.{__name__}_solar_sell_fixed_price": {},
        },
    },
    "input_select":{
        f"battmind_select_release":{
            "options":[
                "latest",
                ],
            "initial":"latest",
            "icon":"mdi:form-select"
        },
        f"{__name__}_consumption_forecast_type":{
            "options":[
                "Average",
                "EMA (Exponential Moving Average)",
                "Trend",
                ],
            "initial":"EMA (Exponential Moving Average)",
            "icon":"mdi:form-select"
        },
    },
    "input_button":{
        f"{__name__}_enforce_planning":{
            "icon":"mdi:calendar-refresh"
        },
        f"{__name__}_restart_script":{
            "icon":"mdi:restart"
        }
    },
    "input_boolean":{
        f"{__name__}_debug_log":{
            "icon":"mdi:math-log"
        },
        f"{__name__}_deactivate_script":{
            "icon": "mdi:cancel"
        },
        f"{__name__}_cheapest_hour_fill_planner":{
            "icon": "mdi:hand-coin"
        },
        f"{__name__}_most_expensive_planner":{
            "icon": "mdi:chart-bar"
        },
        f"{__name__}_only_discharge_on_profit":{
            "icon": "mdi:cash-plus"
        },
        f"{__name__}_prioritize_discharge_hours_by_energy_cost":{
            "icon": "mdi:chart-waterfall"
        },
        f"{__name__}_sell_excess_kwh_available":{
            "icon": "mdi:basket-unfill"
        },
    },
    "input_number":{
        f"{__name__}_kwh_charged_by_solar":{
            "min":0,
            "max":999999,
            "step":0.01,
            "icon":"mdi:white-balance-sunny",
            "unit_of_measurement":"kWh",
            "mode": "box"
        },
        f"{__name__}_solar_sell_fixed_price":{
            "min":-1,
            "max":2,
            "step":0.01,
            "icon":"mdi:cash-multiple",
            "mode": "box"
        },
        f"{__name__}_cheap_price_periods":{
            "min": 1,
            "max": 10,
            "step": 1,
            "icon": "mdi:counter"
        },
        f"{__name__}_cheapest_price_rise_threshold":{
            "min": 0,
            "max": 1,
            "step": 0.01,
            "icon": "mdi:decimal-comma-increase",
            "mode": "box"
        },
        f"{__name__}_cheap_price_period_rise_threshold":{
            "min": 0,
            "max": 1,
            "step": 0.01,
            "icon": "mdi:decimal-comma-increase",
            "mode": "box"
        },
        f"{__name__}_min_profit_per_kwh":{
            "min": 0,
            "max": 10,
            "step": 0.01,
            "mode":"box",
            "unit_of_measurement": "kr",
            "icon": "mdi:cash-plus"
        },
    },
    "input_text":{
        f"{__name__}_exclude_sell_hours":{
            "icon":"mdi:timer-remove-outline",
            "mode": "text",
            "pattern": '^([0-9]|1[0-9]|2[0-3])(,([0-9]|1[0-9]|2[0-3]))*$'
        },
    },
    "template": [
        {
            "sensor": [
                {
                    "default_entity_id": f"sensor.{__name__}_current_charging_rule",
                    "unique_id": f"{__name__}_current_charging_rule",
                    "state": ""
                },
                {
                    "default_entity_id": f"sensor.{__name__}_emoji_description",
                    "unique_id": f"{__name__}_emoji_description",
                    "state": ""
                },
                {
                    "default_entity_id": f"sensor.{__name__}_overview",
                    "unique_id": f"{__name__}_overview",
                    "state": ""
                },
                {
                    "default_entity_id": f"sensor.{__name__}_charging_history",
                    "unique_id": f"{__name__}_charging_history",
                    "state": ""
                },
                {
                    "default_entity_id": f"sensor.{__name__}_powerwall_action",
                    "unique_id": f"{__name__}_powerwall_action",
                    "state": ""
                }
            ]
        }
    ]
}

ENTITIES_RENAMING = {# Old path: New path (seperated by ".")
}

i18n = I18nCatalog(base_lang="en-GB")

def welcome():
    func_name = "welcome"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    
    repo_path = f"{CONFIG_FOLDER}/BattMind"
    local_tag = ""
    try:
        local_tag = get_local_tag(repo_path)
    except:
        pass
    
    return f"BattMind🤵‍♂️🔋⚡ (Script: {__name__}.py) {local_tag}"

def retry(times, exceptions, delay=1.0, backoff=1.0):
    """
    Async retry decorator for PyScript.

    :param times: number of attempts
    :param exceptions: tuple of exceptions to catch
    :param delay: initial delay in seconds
    :param backoff: multiplier for exponential backoff (1 = fixed delay)
    """
    func_name = "retry"
    func_prefix = f"{func_name}_"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    def decorator(func):

        async def newfn(*args, **kwargs):
            attempt = 1
            current_delay = delay

            while attempt <= times:
                try:
                    return await func(*args, **kwargs)

                except exceptions as e:
                    if attempt == times:
                        raise

                    _LOGGER.warning(
                        f"Retry {attempt}/{times} failed in {func.__name__}: {e}. "
                        f"Retrying in {current_delay}s"
                    )

                    await task.wait_until(timeout=current_delay)

                    current_delay *= backoff
                    attempt += 1

        return newfn

    return decorator

def task_wait_until(task_name, timeout=3.0, wait_period=1.0):
    func_name = "task_wait_until"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    global TASKS
    
    try:
        if task_name not in TASKS:
            return True
        
        if not isinstance(TASKS[task_name], asyncio.Task):
            return True
        
        if TASKS[task_name].done():
            return True
        
        period = 0
        while task_name in TASKS and (not TASKS[task_name].done() or not TASKS[task_name].cancelled()):
            task.wait_until(timeout=wait_period)
            period += wait_period
            if period >= timeout:
                break
        
        if task_name in TASKS and TASKS[task_name].done():
            return True
        else:
            return False
    except Exception as e:
        _LOGGER.error(f"Error while waiting for task {task_name} (INSTANCE_ID: {INSTANCE_ID}): {e} {type(e)}")
        
    return False
    
def task_cancel(task_name, task_remove=True, timeout=5.0, wait_period=0.2, startswith=False, contains=False):
    func_name = "task_cancel"
    func_prefix = f"{func_name}_"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    global TASKS

    def _cancel_one(task_key: str) -> bool:
        global TASKS
        _LOGGER = globals()['_LOGGER'].getChild(f"_cancel_one_{task_key}")
        try:
            if task_key not in TASKS:
                return True

            task_obj = TASKS[task_key]

            if not isinstance(task_obj, asyncio.Task):
                if task_remove:
                    TASKS.pop(task_key, None)
                return True

            task_wait_until(task_key, timeout=0.5, wait_period=0.2)

            if ("save" in task_key or "saving" in task_key) and not task_obj.done():
                _LOGGER.warning(f"Waiting 300s for task {task_key} (saving)")
                task_wait_until(task_key, timeout=300.0, wait_period=0.2)
                if not task_obj.done():
                    my_persistent_notification(
                        f"Task {task_key} from instance {INSTANCE_ID} is taking a long time to finish saving. It is recommended to restart Home Assistant to avoid data corruption.",
                        title=f"⚠️ {__name__} - Task Saving Timeout Warning",
                        persistent_notification_id=f"{__name__}_{func_name}_saving_timeout_{INSTANCE_ID}"
                    )

            if "charging_history_worker" in task_key and not task_obj.done():
                _LOGGER.warning(f"Waiting 60s for task {task_key} (charging history)")
                task_wait_until(task_key, timeout=60.0, wait_period=0.2)

            if not task_wait_until(task_key, timeout=1, wait_period=0.2):
                task_obj.cancel()

            if task_wait_until(task_key, timeout=timeout, wait_period=wait_period):
                if task_remove:
                    TASKS.pop(task_key, None)
                return True
            else:
                _LOGGER.error(f"Task {task_key} still not done after timeout (INSTANCE_ID: {INSTANCE_ID})")
                return False

        except Exception as e:
            _LOGGER.error(f"Exception while cancelling {task_key}: {e} (INSTANCE_ID: {INSTANCE_ID})")
            return False

    task_names = []

    if isinstance(task_name, str):
        if startswith:
            task_names = [n for n in TASKS.keys() if n.startswith(task_name)]
        elif contains:
            task_names = [n for n in TASKS.keys() if task_name in n]
        else:
            task_names = [task_name]

    elif isinstance(task_name, Iterable):
        for name in task_name:
            if isinstance(name, asyncio.Task):
                task_names.append(name.get_name())
            elif isinstance(name, str):
                if startswith:
                    task_names.extend([n for n in TASKS.keys() if n.startswith(name)])
                elif contains:
                    task_names.extend([n for n in TASKS.keys() if name in n])
                else:
                    task_names.append(name)
            else:
                _LOGGER.warning(f"Ignoring invalid task name: {name}")

    else:
        _LOGGER.error(f"Invalid type for task_name: {type(task_name)}")
        return False

    if not task_names:
        if startswith:
            _LOGGER.debug(f"No matching tasks to cancel with prefix: {task_name}")
        elif contains:
            _LOGGER.debug(f"No matching tasks to cancel containing: {task_name}")
        else:
            _LOGGER.debug(f"No matching tasks to cancel with name: {task_name}")
        return True

    task_set = set()
    for name in set(task_names):
        TASKS[f"{func_prefix}_cancel_one_{name}"] = task.create(_cancel_one, name)
        TASKS[f"{func_prefix}_cancel_one_{name}"].set_name(f"{func_prefix}_cancel_one_{name}")
        task_set.add(TASKS[f"{func_prefix}_cancel_one_{name}"])
    
    done, pending = task.wait(task_set)

    all_success = True
    for task_job in task_set:
        name = task_job.get_name()
        
        try:
            result = TASKS[name].result()
            if result is not True:
                _LOGGER.warning(f"Task cancel failed for: {name}")
                all_success = False
        except (asyncio.CancelledError, asyncio.TimeoutError, KeyError) as e:
            pass
        except Exception as e:
            _LOGGER.error(f"Exception cancelling task {name}: {e} {type(e)}")
            all_success = False
        finally:
            TASKS.pop(name, None)

    return all_success

def task_shutdown():
    func_name = "task_shutdown"
    task.unique(func_name)
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    global TASKS

    start_ts = datetime_to_unix()
    timeout = 5 * 60.0

    tasks_done_list = []
    task_length = len(TASKS)

    async def cancel_and_mark(task_name: str):
        if task_cancel(task_name, task_remove=False):
            tasks_done_list.append(task_name)

    running = {task.create(cancel_and_mark, name) for name in list(TASKS.keys())}

    while running and (datetime_to_unix() - start_ts) < timeout:
        running = {t for t in running if not t.done()}

        finished = task_length - len(running)
        set_charging_rule(f"📟{i18n.t('ui.tasks.closing_progress')} {finished}/{task_length}")

        if running:
            task.wait_until(timeout=0.2)

    if running:
        hanging = len(running)
        _LOGGER.error(f"Timeout: {hanging} tasks kunne ikke lukkes inden 5 minutter")
        set_charging_rule(f"📟{i18n.t('ui.tasks.not_all_closed')} (timeout)")
    else:
        set_charging_rule(f"📟{i18n.t('ui.tasks.closed_done')}")

    for task_name in tasks_done_list:
        TASKS.pop(task_name, None)

    if TASKS:
        for task_name in list(TASKS.keys()):
            if TASKS[task_name].done() or TASKS[task_name].cancelled():
                TASKS.pop(task_name, None)
        if TASKS:
            _LOGGER.warning(f"Some tasks were not killed from instance {INSTANCE_ID}:\n{pformat(TASKS, indent=4, width=80)}")

    task.wait_until(timeout=0.5)
    TASKS = {}

def wait_for_entity_update(entity_id=None, updated_within_minutes=5, max_wait_time_minutes=5, check_interval=5.0, float_type=False, task_prefix=None, task_name=None) -> bool:
    """
    Wait until a Home Assistant entity has updated its state within a given timeframe.

    The function checks the entity's historical values (last 24h) and waits until it
    receives a new update that is more recent than the last known state. If the entity
    already has an update within the `updated_within_minutes` window, the function
    returns immediately.

    Task naming rules:
        - If both `task_prefix` and `task_name` are None, a default name is generated
          using the function name and the entity_id.
        - If both `task_prefix` and `task_name` are provided, the task will NOT be
          automatically removed when finished.
        - If only one of `task_prefix` or `task_name` is provided, the other will be
          set to a default value based on the function name, and the task will be
          removed when done.

    Args:
        entity_id (str): The entity ID to monitor.
        updated_within_minutes (int): If the entity has been updated within this time
            window (minutes), the function returns immediately.
        max_wait_time_minutes (int): Maximum number of minutes to wait before giving up.
        check_interval (float): Interval in seconds to re-check for updates.
        float_type (bool): Whether to treat the entity state as float when fetching values.
        task_prefix (str, optional): Optional prefix for the task name.
        task_name (str, optional): Optional base name for the task.

    Returns:
        bool:
            - True if the entity has a new state update within the specified time frame.
            - False if no new update is detected before timeout,
              or if the task is cancelled/errored.

    Notes:
        - The function looks at historical values from the last 24 hours using `get_values`.
        - A background task entry is created in TASKS to track cancellation and cleanup.
        - If `task_prefix` and `task_name` are auto-generated, the task will be
          cancelled/removed in the `finally` block when done.
    """
    func_name = "wait_for_entity_update"
    func_prefix = f"{func_name}_"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    
    global TASKS
    
    remove_task_when_done = False
        
    if entity_id is None:
        _LOGGER.error("No entity_id provided")
        return False
    
    if isinstance(task_prefix, str) and isinstance(task_name, str):
        full_task_name = f"{task_prefix}{task_name}"
    else:
        if task_prefix is None:
            task_prefix = func_prefix
        
        if task_name is None:
            task_name = func_name
        
        full_task_name = f"{task_prefix}{task_name}_{entity_id}"
        remove_task_when_done = True
    
    if full_task_name not in TASKS:
        TASKS[full_task_name] = task.current_task()
    
    def _conditions():
        task_obj = TASKS.get(full_task_name)
        if not task_obj:
            return False
        if task_obj.cancelled() or task_obj.cancelling():
            return False
        return (getTime() - start_time) < datetime.timedelta(minutes=max_wait_time_minutes)
    
    try:
        to_datetime = getTime()
        from_datetime = to_datetime - datetime.timedelta(hours=24)
        values = get_values(entity_id, from_datetime, to_datetime, float_type=float_type, convert_to=None, include_timestamps=True, error_state={})
        if not values or not isinstance(values, dict) or not values.items():
            _LOGGER.warning(f"No state values found in the last 24 hours for {entity_id}")
            return False
        
        values_sorted = sorted(values.items(), reverse=True)
        last_state = values_sorted[1][1] if values_sorted else 0.0
        last_state_timestamp = values_sorted[1][0] if values_sorted else getTime()
        
        start_time = getTime()
        
        if minutesBetween(last_state_timestamp, start_time) < updated_within_minutes:
            _LOGGER.info(f"{entity_id} state up to date {int(last_state)} {last_state_timestamp}, no need to wait")
            return True
        
        while _conditions():
            to_datetime = getTime()
            from_datetime = to_datetime - datetime.timedelta(hours=24)
            values = get_values(entity_id, from_datetime, to_datetime, float_type=float_type, convert_to=None, include_timestamps=True, error_state={})
            
            if not values or not isinstance(values, dict) or not values.items():
                _LOGGER.warning(f"No state values found in the last 24 hours for {entity_id}")
                return False
            
            values_sorted = sorted(values.items(), reverse=True)
            if values_sorted:
                current_state = values_sorted[1][1]
                current_state_timestamp = values_sorted[1][0]
                if current_state_timestamp > last_state_timestamp:
                    _LOGGER.info(f"{entity_id} state stable at {current_state} now, no need to wait anymore")
                    return True
            _LOGGER.info(f"Waiting for {entity_id} to update, last state {int(last_state)} at {last_state_timestamp}, now {getTime()}")
            task.wait_until(timeout=check_interval)
    except (asyncio.CancelledError, asyncio.TimeoutError, KeyError) as e:
        _LOGGER.warning(f"Task was cancelled or timed out in {func_name}: {e} {type(e)}")
    except Exception as e:
        _LOGGER.error(f"Error in {func_name}: {e} {type(e)}")
    finally:
        if remove_task_when_done:
            task_cancel(f"{task_prefix}{task_name}")
    return False

def calculate_price_levels(prices):
    """ Calculates price levels based on the provided prices. """
    lowest_price = min(prices)
    highest_price = max(prices)
    mean_price = sum(prices) / len(prices)
    step_under = (mean_price - lowest_price) / 5
    step_over = (highest_price - mean_price) / 5

    return {
        "price1": lowest_price,
        "price2": mean_price - step_under * 4,
        "price3": mean_price - step_under * 3,
        "price4": mean_price - step_under * 2,
        "price5": mean_price - step_under * 1,
        "price6": mean_price,
        "price7": mean_price + step_over * 1,
        "price8": mean_price + step_over * 2,
        "price9": mean_price + step_over * 3,
        "price10": mean_price + step_over * 4,
        "price11": highest_price,
    }

def append_overview_output(title = None, timestamp = None):
    func_name = "append_overview_output"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    global OVERVIEW_HISTORY
    
    if timestamp is None:
        timestamp = getTime().strftime("%Y-%m-%d %H:%M")
    
    key = f"{timestamp} {title}"
    
    if key in OVERVIEW_HISTORY:
        return
    
    OVERVIEW_HISTORY[key] = [get_attr(f"sensor.{__name__}_overview", "overview", error_state=f"Cant get sensor.{__name__}_overview.overview")]
    
    OVERVIEW_HISTORY = limit_dict_size(OVERVIEW_HISTORY, 10)
    
def format_debug_value(value):
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return value.isoformat()
    elif isinstance(value, datetime.timedelta):
        return str(value)
    elif isinstance(value, dict):
        return {
            format_debug_value(k): format_debug_value(v)
            for k, v in value.items()
        }
    elif isinstance(value, list):
        return [format_debug_value(v) for v in value]
    elif isinstance(value, tuple):
        return tuple([format_debug_value(v) for v in value])
    elif isinstance(value, asyncio.Task):
        return str(value)
    return value

def format_debug_table(table):
    return {k: format_debug_value(v) for k, v in table.items()} if table else None

def format_debug_details(details):
    return {k: format_debug_value(v) for k, v in details.items()} if details else None

def get_debug_info_sections():
    return {
        "System Status": {
            "table": format_debug_table({
                "INITIALIZATION_COMPLETE": INITIALIZATION_COMPLETE,
                "TESTING": TESTING,
            }),
            "details": None,
        },
        "System Configuration": {
            "table": format_debug_table({
                "SOLAR_CONFIGURED": SOLAR_CONFIGURED,
                "POWERWALL_CONFIGURED": POWERWALL_CONFIGURED,
                "CONFIG_LAST_MODIFIED": datetime.datetime.fromtimestamp(CONFIG_LAST_MODIFIED) if isinstance(CONFIG_LAST_MODIFIED, (float, int)) else CONFIG_LAST_MODIFIED,
            }),
            "details": format_debug_details({"CONFIG": CONFIG}),
        },
        "Database": {
            "table": None,
            "details": format_debug_details({
                "POWER_VALUES_DB": POWER_VALUES_DB,
                "SOLAR_PRODUCTION_AVAILABLE_DB": SOLAR_PRODUCTION_AVAILABLE_DB,
                }),
        },
        "Charging Plan & Price Logic": {
            "table": format_debug_table({
                "CURRENT_CHARGING_AMPS": CURRENT_CHARGING_AMPS,
            }),
            "details": format_debug_details({
                "CHARGING_PLAN": CHARGING_PLAN,
                "CHARGE_HOURS": CHARGE_HOURS,
                "LAST_SUCCESSFUL_GRID_PRICES": LAST_SUCCESSFUL_GRID_PRICES,
            }),
        },
        "Charging Expenses": {
            "table": None,
            "details": format_debug_details({"BATTERY_LEVEL_EXPENSES": BATTERY_LEVEL_EXPENSES}),
        },
        "Local Energy Forecast": {
            "table": None,
            "details": format_debug_details({
                "LOCAL_ENERGY_PRICES": LOCAL_ENERGY_PRICES,
                "LOCAL_ENERGY_PREDICTION_DB": LOCAL_ENERGY_PREDICTION_DB,
            }),
        },
        "Charging Sessions & History": {
            "table": format_debug_table({
                "INTEGRATION_OFFLINE_TIMESTAMP": INTEGRATION_OFFLINE_TIMESTAMP,
                "CHARGING_HISTORY_ENDING_BYTE_SIZE": CHARGING_HISTORY_ENDING_BYTE_SIZE,
            }),
            "details": None,
        },
        "Runtime Counters & Errors": {
            "table": format_debug_table({
                "CHARGING_IS_BEGINNING": CHARGING_IS_BEGINNING,
                "ERROR_COUNT": ERROR_COUNT,
                "RESTARTING_CHARGER": RESTARTING_CHARGER,
            }),
            "details": None,
        },
        "Task Runtime State": {
            "table": format_debug_table({
                "INSTANCE_ID": INSTANCE_ID,
                "TASKS_COUNT": len(TASKS) if TASKS else 0,
            }),
            "details": format_debug_details({"TASKS": TASKS}),
        },
        "Tariff Settings": {
            "table": None,
            "details": format_debug_details({"SOLAR_SELL_TARIFF": SOLAR_SELL_TARIFF}),
        },
    }

def run_console_command_sync(cmd):
    try:
        result = task.executor(subprocess.run, cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=15)
        if result.returncode != 0:
            raise RuntimeError(f"Git failed: {result.stderr.strip()}")
        return result.stdout.strip()
    except subprocess.TimeoutExpired:
        result.kill()
        raise TimeoutError("Git command timed out after 15 seconds")
    except RuntimeError as e:
        if "Git failed: fatal: detected dubious ownership in repository at" not in str(e):
            raise RuntimeError(f"Git command failed: {e} {type(e)}")
    except Exception as e:
        raise RuntimeError(f"Unexpected error: {e} {type(e)}")

async def run_console_command(cmd):
    """Run a git command synchronously and safely with timeout and error handling."""
    try:
        length = 8
        random_string = ''.join(random.choices(string.ascii_letters + string.digits, k=length))
        
        task_name = f'run_console_command_{random_string}'
        TASKS[task_name] = task.create(run_console_command_sync, cmd)
        done, pending = task.wait({TASKS[task_name]})
        result = TASKS[task_name].result()
        
        task_cancel(task_name, task_remove=True, timeout=5.0, wait_period=0.2)
        
        return result
    except TimeoutError as e:
        raise TimeoutError(e)
    except RuntimeError as e:
        raise RuntimeError(e)
    except Exception as e:
        raise RuntimeError(e)

@service(f"pyscript.{__name__}_recreate_hardlinks")
def recreate_hardlinks(trigger_type=None, trigger_id=None, **kwargs):
    """yaml
    name: "BattMind: Recreate Hardlinks"
    description: >
        Recreate hardlinks for BattMind data storage (use if you encounter issues with data not being saved or loaded correctly).
        Can be safely run multiple times.
        If you have configured a notification service, you will receive a notification with the result.
        Otherwise, the output will be logged.
    """
    func_name = "recreate_hardlinks"
    task.unique(func_name)
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    
    output = run_console_command(["bash", f"{CONFIG_FOLDER}/BattMind/scripts/recreate_hardlinks_battmind.sh"])
    if trigger_type != "service":
        return output
    
    if output:
        my_persistent_notification(
            f"**{i18n.t('ui.recreate_hardlinks.success')}:**\n\n{output}",
            title=f"{TITLE} Hardlinks",
            persistent_notification_id=f"{__name__}_recreate_hardlinks"
        )
    else:
        my_persistent_notification(
            f"**{i18n.t('ui.recreate_hardlinks.error', error=str(output))}:**\n\n{output}",
            title=f"{TITLE} Hardlinks Error",
            persistent_notification_id=f"{__name__}_recreate_hardlinks_error"
        )

def get_github_releases(repo_owner="dezito", repo_name="BattMind"):
    """Fetch all releases from GitHub (newest first)."""
    api_url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/releases"
    output = run_console_command_sync(["curl", "-s", api_url])
    data = json.loads(output)
    if not isinstance(data, list):
        raise ValueError(f"Unexpected GitHub API format: {type(data)}")
    return data

def get_local_tag(repo_path):
    """Return the current local Git tag or fallback to v0.0.0."""
    try:
        run_console_command(["git", "-C", repo_path, "fetch", "--tags"])
        tag = run_console_command_sync(["git", "-C", repo_path, "describe", "--tags", "--abbrev=0"])
        tag = tag.strip() if tag else None
        
        if not tag or tag.lower() == "none":
            return "v0.0.0"
        return tag
    except Exception as e:
        _LOGGER.warning(f"Could not determine local tag: {e}")
        return "v0.0.0"

def get_newer_releases(releases, local_tag, target_tag=None):
    """
    Return all releases newer than the local tag (up to target tag if given).
    Newest → Oldest.
    """
    newer = []
    for rel in reverse_list(releases):
        tag = rel.get("tag_name", "").strip()
        _LOGGER.info(f"Checking release tag: {tag}")
        if tag == local_tag:
            continue
        newer.append(rel)
        if target_tag and tag == target_tag:
            break
    if not newer:
        newer = releases
    return newer

def build_combined_changelog(releases):
    """Combine multiple GitHub release notes into a Markdown-formatted changelog (newest first)."""
    items = []
    for rel in reverse_list(releases):
        tag = rel.get("tag_name", "")
        name = rel.get("name", tag)
        body = rel.get("body", "").strip().replace("##", "###").replace("What's Changed", f"{name or tag} {i18n.t('ui.repo.new_update_available_changes').lower()}:") or f"{name or tag} {i18n.t('ui.repo.no_release_notes')}:"
        url = rel.get("html_url", "")
        section = (
            f"{body}\n\n"
            f"[{i18n.t('ui.repo.view_on_github')}]({url})\n\n"
            f"---"
        )
        items.append(section)
    return "\n\n".join(items)

@service(f"pyscript.{__name__}_check_release_updates")
def check_release_updates(trigger_type=None, trigger_id=None, **kwargs):
    """yaml
    name: "BattMind: Check for Updates"
    description: >
         Check if newer releases are available on GitHub and show a combined changelog of all new releases.
         If you have configured a notification service, you will receive a notification with the result.
         Otherwise, the output will be logged.
     """
    func_name = "check_release_updates"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)

    repo_path = f"{CONFIG_FOLDER}/BattMind"
    selected_version = get_state("input_select.battmind_select_release", error_state="").strip()
    try:
        releases = get_github_releases()
        local_tag = get_local_tag(repo_path)

        if not selected_version or selected_version.lower() == "latest":
            target_release = releases[0]
        else:
            target_release = None
            for r in releases:
                if r.get("tag_name", "") == selected_version:
                    target_release = r
                    break
            if not target_release:
                raise ValueError(f"Release '{selected_version}' not found")
        
        target_tag = target_release.get("tag_name", "").strip()
        _LOGGER.info(f"Local: {local_tag} | Target: {target_tag}")

        if local_tag == target_tag:
            my_persistent_notification(
                f"✅ {i18n.t('ui.repo.no_updates_available')} ({local_tag})",
                title=f"{TITLE} {i18n.t('ui.repo.no_updates_available_title')}",
                persistent_notification_id=f"{__name__}_{func_name}"
            )
            return

        newer_releases = get_newer_releases(releases, local_tag, target_tag)
        changelog = build_combined_changelog(newer_releases)
        if len(changelog) > 5000:
            changelog = changelog[:5000] + "\n* …"

        my_persistent_notification(
            f"### ➡️ {i18n.t('ui.repo.current')}: `{local_tag}`\n"
            f"### ➡️ {i18n.t('ui.repo.latest')}: `{target_tag}`\n\n"
            f"## 📌 **{i18n.t('ui.repo.new_update_available_changes')}:**\n\n{changelog}",
            title=f"{TITLE} {i18n.t('ui.repo.new_update_available_title')} ({len(newer_releases)} releases)",
            persistent_notification_id=f"{__name__}_{func_name}"
        )

    except Exception as e:
        _LOGGER.error(f"Update check failed: {e}")
        my_persistent_notification(
            f"⚠️ {i18n.t('ui.repo.update_error_check')}: {e}",
            title=f"{TITLE} Error",
            persistent_notification_id=f"{__name__}_{func_name}_error"
        )

@service(f"pyscript.{__name__}_update_repo")
def update_repo(trigger_type=None, trigger_id=None, **kwargs):
    """yaml
    name: "BattMind: Update to Selected Release"
    description: >
         Update the local repository to the selected release version from GitHub and show a combined changelog of all new releases included in the update.
         If you have configured a notification service, you will receive a notification with the result.
         Otherwise, the output will be logged.
    """
    func_name = "update_repo"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)

    repo_path = f"{CONFIG_FOLDER}/BattMind"
    selected_version = get_state("input_select.battmind_select_release", error_state="").strip()
    try:
        releases = get_github_releases()
        local_tag = get_local_tag(repo_path)

        if not selected_version or selected_version.lower() == "latest":
            target_release = releases[0]
        else:
            target_release = None
            for r in releases:
                if r.get("tag_name", "") == selected_version:
                    target_release = r
                    break
            if not target_release:
                raise ValueError(f"Release '{selected_version}' not found")

        target_tag = target_release.get("tag_name", "").strip()
        _LOGGER.info(f"Local: {local_tag} | Target: {target_tag}")

        if local_tag == target_tag:
            my_persistent_notification(
                f"✅ {i18n.t('ui.repo.no_updates_available')} ({local_tag})",
                title=f"{TITLE} {i18n.t('ui.repo.no_updates_available_title')}",
                persistent_notification_id=f"{__name__}_{func_name}"
            )
            return

        newer_releases = get_newer_releases(releases, local_tag, target_tag)
        changelog = build_combined_changelog(newer_releases)
        if len(changelog) > 5000:
            changelog = changelog[:5000] + "\n* …"

        # --- perform actual update ---
        run_console_command(["git", "-C", repo_path, "fetch", "--tags"])
        run_console_command(["git", "-C", repo_path, "checkout", "-f", target_tag])
        recreate_hardlinks()

        message = (
            f"### {i18n.t('ui.repo.updated_from')} `{local_tag}` → `{target_tag}`\n\n"
            f"## 📌 **{i18n.t('ui.repo.new_update_available_changes')}:**\n\n{changelog}"
        )

        my_persistent_notification(
            message,
            title=f"{TITLE} {i18n.t('ui.repo.updated_success')} ({len(newer_releases)} releases)",
            persistent_notification_id=f"{__name__}_{func_name}"
        )
        restart_script()

    except Exception as e:
        _LOGGER.error(f"Update failed: {e}")
        my_persistent_notification(
            f"⚠️ {i18n.t('ui.repo.update_error')}: {e}",
            title=f"{TITLE} Error",
            persistent_notification_id=f"{__name__}_{func_name}_error"
        )

@service(f"pyscript.{__name__}_debug_info")
def debug_info(trigger_type=None, trigger_id=None, **kwargs):
    """yaml
    name: "BattMind: Show Debug Info"
    description: >
        Show debug info about the current state of the system, configuration, and runtime variables.
        If you have configured a notification service, you will receive a notification with the result.
        Otherwise, the output will be logged.
    """
    func_name = "debug_info"
    task.unique(func_name)
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    debug_info = []

    debug_info.append(f"<center>\n")
    debug_info.append(f"#### Debug info runtime {getTime()} ####\n")
    debug_info.append(f"</center>\n")

    for section, content in get_debug_info_sections().items():
        debug_info.append(f"<center>\n\n### {section}\n</center>\n")
        if content["table"]:
            debug_info.append("| Variable | Value |")
            debug_info.append("|---:|:---|")
            for key, value in content["table"].items():
                debug_info.append(f"| {key}: | {value} |")
                
        if content["table"] and content["details"]:
            debug_info.append("<br>\n")
        
        if content["details"]:
            for detail_key, detail_value in content["details"].items():
                debug_info.append("<details>")
                debug_info.append(f"<summary>{detail_key}: Show dictionary</summary>\n")
                debug_info.append(f"```\n{pformat(detail_value)}\n```")
                debug_info.append("</details>\n")
                if len(content["details"]) > 1:
                    debug_info.append("<br>\n")
        debug_info.append("---")
        
    if "debug" in kwargs and kwargs["debug"]:
        debug_info.append(f"<center>\n")
        debug_info.append(f"#### Debug info function ####\n")
        debug_info.append(f"</center>\n")

        for section, content in kwargs["debug"].items():
            debug_info.append(f"<center>\n\n### {section}\n</center>\n")
            if content["table"]:
                debug_info.append("| Variable | Value |")
                debug_info.append("|---:|:---|")
                for key, value in content["table"].items():
                    debug_info.append(f"| {key}: | {value} |")
                    
            if content["table"] and content["details"]:
                debug_info.append("<br>\n")
            
            if content["details"]:
                for detail_key, detail_value in content["details"].items():
                    debug_info.append("<details>")
                    debug_info.append(f"<summary>{detail_key}: Show dictionary</summary>\n")
                    debug_info.append(f"```\n{pformat(detail_value)}\n```")
                    debug_info.append("</details>\n")
                    if len(content["details"]) > 1:
                        debug_info.append("<br>\n")
            debug_info.append("---")
    
    if OVERVIEW_HISTORY:
        debug_info.append(f"<center>\n\n### Overview History\n</center>\n")
        for title, overview in sorted(OVERVIEW_HISTORY.items(), reverse=True):
            debug_info.append("<details>")
            debug_info.append(f"<summary>{title}: Show snapshot</summary>\n")
            debug_info.extend(overview)
            debug_info.append("</details>\n")

    debug_info_output = "\n".join(debug_info)

    #_LOGGER.info(f"Debug Info: \n{get_debug_info_sections()}")
    my_persistent_notification(
        debug_info_output,
        title=f"{TITLE} debug info",
        persistent_notification_id=f"{__name__}_{func_name}"
    )

def save_error_to_file(error_message, debug=None, caller_function_name = None):
    func_name = "save_error_to_file"
    func_prefix = f"{func_name}_"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    global TASKS
    return
    
    def convert_tuples_to_lists(obj):
        if isinstance(obj, tuple):
            return list(obj)
        elif isinstance(obj, dict):
            return {k: convert_tuples_to_lists(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [convert_tuples_to_lists(item) for item in obj]
        else:
            return obj
        
    def remove_anchors(data):
        if isinstance(data, list):
            return [remove_anchors(item) for item in data]
        elif isinstance(data, dict):
            return {key: remove_anchors(value) for key, value in data.items()}
        else:
            return data
    
    filename = f"{__name__}_error_log.yaml"
    
    try:
        TASKS[f"{func_prefix}get_debug_info_sections"] = task.create(get_debug_info_sections)
        TASKS[f'{func_prefix}error_log'] = task.create(load_yaml, filename)
        done, pending = task.wait({TASKS[f'{func_prefix}error_log'], TASKS[f"{func_prefix}get_debug_info_sections"]})
        
        error_log = deepcopy(TASKS[f'{func_prefix}error_log'].result())
        debug_info_sections = deepcopy(TASKS[f"{func_prefix}get_debug_info_sections"].result())
        
        if not isinstance(error_log, dict):
            _LOGGER.warning(f"Error log file {filename} is not a dictionary, resetting to empty dictionary")
            error_log = dict()
        
        TASKS[f"{func_prefix}convert_tuples_to_lists"] = task.create(convert_tuples_to_lists, debug_info_sections)
        done, pending = task.wait({TASKS[f"{func_prefix}convert_tuples_to_lists"]})
        
        live_image = deepcopy(TASKS[f"{func_prefix}convert_tuples_to_lists"].result())
        
        debug_dict = {
            "caller_function_name": caller_function_name,
            "error_message": error_message,
            "debug": debug,
            "live_image": live_image,
        }
        
        if error_log:
            error_log = limit_dict_size(error_log, 10)
            
            for timestamp in sorted(list(error_log.keys()), reverse=True):
                if minutesBetween(timestamp, getTime()) < 60:
                    _LOGGER.warning("Ignoring error log, as there is already an error logged within the last hour")
                    return
                break
                
        TASKS[f"{func_prefix}remove_anchors"] = task.create(remove_anchors, debug_dict)
        done, pending = task.wait({TASKS[f"{func_prefix}remove_anchors"]})
        error_log[getTime()] = deepcopy(TASKS[f"{func_prefix}remove_anchors"].result())
        
        TASKS[f"{func_prefix}save_changes"] = task.create(save_changes, filename, deepcopy(error_log))
        done, pending = task.wait({TASKS[f"{func_prefix}save_changes"]})
    except (asyncio.CancelledError, asyncio.TimeoutError, KeyError) as e:
        _LOGGER.warning(f"Task cancelled or timeout: {e} {type(e)}")
        return
    except Exception as e:
        _LOGGER.error(f"Error saving error to file error_message: {error_message} caller_function_name: {caller_function_name}: {e} {type(e)}")
    finally:
        task_cancel(func_prefix, task_remove=True, timeout=5.0, startswith=True)

def is_solar_configured(cfg = None):
    func_name = "is_solar_configured"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    global SOLAR_CONFIGURED
    
    def check_criteria(cfg):
        return True if cfg['solar']['entity_ids']['production_entity_id'] else False
    
    if cfg is not None:
        return check_criteria(cfg)
    
    if SOLAR_CONFIGURED is None:
        SOLAR_CONFIGURED = check_criteria(CONFIG)
        _LOGGER.info(f"Solar entity is {'' if SOLAR_CONFIGURED else 'not '}configured")
    
    return SOLAR_CONFIGURED

def is_powerwall_configured(cfg = None):
    func_name = "is_powerwall_configured"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    global POWERWALL_CONFIGURED
    
    def check_criteria(cfg):
        return True if cfg['home']['entity_ids']['powerwall_watt_flow_entity_id'] else False
    
    if cfg is not None:
        return check_criteria(cfg)
    
    if POWERWALL_CONFIGURED is None:
        POWERWALL_CONFIGURED = check_criteria(CONFIG)
        _LOGGER.info(f"Powerwall entity is {'' if POWERWALL_CONFIGURED else 'not '}configured")
    
    return POWERWALL_CONFIGURED

def is_entity_configured(entity):
    func_name = "is_entity_configured"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    if entity is None or entity == "":
        return False
    return True

def is_entity_available(entity):
    func_name = "is_entity_available"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    global INTEGRATION_OFFLINE_TIMESTAMP
    
    if not is_entity_configured(entity):
        return
    
    integration = get_integration(entity)
    
    try:
        entity_state = str(get_state(entity, error_state="unknown"))
        if entity_state in ENTITY_UNAVAILABLE_STATES:
            raise Exception(f"Entity state is {entity_state}")
        
        if integration is not None:
            if integration in INTEGRATION_OFFLINE_TIMESTAMP:
                del INTEGRATION_OFFLINE_TIMESTAMP[integration]
                _LOGGER.warning(f"Removing {integration} from offline timestamp")
                
        return True
    except Exception as e:
        _LOGGER.warning(f"Entity {entity} not available: {e} {type(e)}")
        
        if integration is not None:
            if integration not in INTEGRATION_OFFLINE_TIMESTAMP:
                INTEGRATION_OFFLINE_TIMESTAMP[integration] = getTime()
                _LOGGER.warning(f"Adding {integration} to offline timestamp")
                
            if minutesBetween(INTEGRATION_OFFLINE_TIMESTAMP[integration], getTime()) > 30:
                _LOGGER.warning(f"Reloading {integration} integration")
                fmt = {
                    "entity": entity,
                    "timestamp": INTEGRATION_OFFLINE_TIMESTAMP[integration]
                }
                my_persistent_notification(
                    f"⛔{i18n.t('ui.entity_unavailable.message', **fmt)}\n\n"
                    f"{i18n.t('ui.entity_unavailable.restart_integration')}",
                    title=f"{TITLE} {i18n.t('ui.entity_unavailable.title')}",
                    persistent_notification_id=f"{__name__}_{func_name}_{entity}"
                )

def save_changes(file, db):
    func_name = "save_changes"
    func_prefix = f"{func_name}_"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    global COMMENT_DB_YAML
    
    db = deepcopy(db) if isinstance(db, (dict, list)) else db
    
    if db == {} or db == [] or db is None:
        _LOGGER.error(f"Database is empty or None for {file}, not saving changes.")
    
    try:
        TASKS[f'{func_prefix}db_disk_{file}'] = task.create(load_yaml, file)
        done, pending = task.wait({TASKS[f'{func_prefix}db_disk_{file}']})
        db_disk = TASKS[f'{func_prefix}db_disk_{file}'].result()
        
        if not isinstance(db_disk, (dict, list)):
            raise Exception(f"Database on disk is not a dictionary for {file}, got {type(db_disk)}")
    except (asyncio.CancelledError, asyncio.TimeoutError, KeyError) as e:
        _LOGGER.warning(f"Task cancelled or timeout: {e} {type(e)}")
        return
    except Exception as e:
        _LOGGER.error(f"Error loading {file} from disk: {e} {type(e)}")
        db_disk = {}
    finally:
        task_cancel(f'{func_prefix}db_disk_{file}', task_remove=True, timeout=5.0)
    
    if "version" in db_disk:
        del db_disk["version"]
    
    comment_db = deepcopy(COMMENT_DB_YAML) if f"{__name__}_config" in file else None
    if not dicts_equal(db, db_disk):
        try:
            _LOGGER.info(f"Saving {file} to disk")
            TASKS[f'{func_prefix}save_yaml_{file}'] = task.create(save_yaml, file, db, comment_db=comment_db)
            done, pending = task.wait({TASKS[f'{func_prefix}save_yaml_{file}']})
        except (asyncio.CancelledError, asyncio.TimeoutError, KeyError) as e:
            _LOGGER.warning(f"Task cancelled or timeout: {e} {type(e)}")
            return
        except Exception as e:
            _LOGGER.error(f"Cant save {file}: {e} {type(e)}")
            my_persistent_notification(
                f"Error saving data to file: {file}\n\n"
                f"Error: {e} {type(e)}",
                f"{TITLE} Error",
                persistent_notification_id=f"{__name__}_{file}_{func_name}_error"
            )
        finally:
            task_cancel(f'{func_prefix}save_yaml_{file}', task_remove=True, timeout=5.0)

def set_charging_rule(text=""):
    func_name = "set_charging_rule"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    global TESTING
    
    testing = "🧪" if TESTING else ""
    
    text = "\n".join([f"{testing}{line.strip()}{testing}" for line in text.split("\n") if isinstance(line, str) and line.strip()])
    try:
        set_state(f"sensor.{__name__}_current_charging_rule", text)
    except Exception as e:
        _LOGGER.warning(f"Cant set sensor.{__name__}_current_charging_rule to '{text}': {e} {type(e)}")
            
def restart_script():
    func_name = "restart_script"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    
    _LOGGER.info("Restarting script")
    set_charging_rule(f"📟{i18n.t('ui.restart_script.message')}")
    if service.has_service("pyscript", "reload"):
        service.call("pyscript", "reload", blocking=True, global_ctx=f"file.{__name__}")

def notify_critical_change(cfg = {}, filename = None):
    func_name = "notify_critical_change"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    
    def check_nested_keys_exist(cfg, dotted_keys):
        missing_keys = []
        for dotted_key in dotted_keys:
            keys = dotted_key.split(".")
            current = cfg
            for key in keys:
                if isinstance(current, dict) and key in current:
                    current = current[key]
                else:
                    missing_keys.append(dotted_key)
                    break
        return missing_keys
    
    def flatten_default_entities_to_desc(default_entities: Dict[str, Any]) -> Dict[str, str]:
        index: Dict[str, str] = {}

        def _full_id(domain: str, key: str) -> str:
            return key if "." in key else f"{domain}.{key}"

        def add_new_template(template_list: Any) -> Dict[str, str]:
            dt: Dict[str, str] = {}
            if not isinstance(template_list, list):
                return dt

            supported_domains = (
                "binary_sensor",
                "sensor",
                "switch",
                "number",
                "select",
            )

            for tmpl in template_list:
                if not isinstance(tmpl, dict):
                    continue

                for domain in supported_domains:
                    entities = tmpl.get(domain)
                    if not isinstance(entities, list):
                        continue

                    for attrs in entities:
                        if not isinstance(attrs, dict):
                            continue

                        key = (
                            attrs.get("default_entity_id")
                            or attrs.get("entity_id")
                            or attrs.get("name")
                        )
                        if not key:
                            continue

                        eid = key if "." in key else _full_id(domain, key)

                        desc = attrs.get("description", "")
                        dt[eid] = desc

            return dt

        try:
            template_list = default_entities.get("template", [])
            index.update(add_new_template(template_list))

            ha = default_entities.get("homeassistant", {})
            if isinstance(ha, dict):
                customize = ha.get("customize", {})
                if isinstance(customize, dict):
                    for full_id, attrs in customize.items():
                        if isinstance(attrs, dict):
                            desc = attrs.get("description")
                            if isinstance(desc, str):
                                index[full_id] = desc

        except Exception as e:
            _LOGGER.error(f"Error flattening default entities to description: {e} {type(e)}")

        return index

    def keys_description(keys: List[str]) -> List[str]:
        """
        Return markdown blocks for each key:
        "**<key>**\\n<description>\\n\\n"
        Lookup order:
        1) COMMENT_DB_YAML[key]
        2) flatten(DEFAULT_ENTITIES)[key]
        3) "" (empty if no description found)
        """
        blocks: List[str] = []
        try:
            entity_desc_index = flatten_default_entities_to_desc(DEFAULT_ENTITIES)

            for key in keys:
                desc = COMMENT_DB_YAML.get(key) or entity_desc_index.get(key, "")
                blocks.append(f"**{key}**\n{desc}\n\n")
        except Exception as e:
            _LOGGER.error(f"Error generating keys description: {e} {type(e)}")
        return blocks

    
    if filename == f"{__name__}_config.yaml":
        pass
            
    if filename == f"packages/{__name__}.yaml":
        if __name__ == "battmind":
            battmind_select_release_entity = [f"input_select.battmind_select_release"]
            battmind_select_release_missing = check_nested_keys_exist(cfg, battmind_select_release_entity)
            
            if battmind_select_release_missing:
                _LOGGER.warning(f"BattMind select release entity update required: {battmind_select_release_missing}")
                
                my_persistent_notification(
                    f"## {i18n.t('ui.notify_critical_change.important_header')}\n\n"
                    f"{i18n.t('ui.notify_critical_change.new_entities_added')}\n"
                    f"{i18n.t('ui.notify_critical_change.entity_description')}\n\n"
                    f"### {i18n.t('ui.notify_critical_change.new_entities')}:\n - {'- '.join(keys_description(battmind_select_release_missing))}\n\n"
                    f"**{i18n.t('ui.notify_critical_change.action')}:**\n"
                    f"{i18n.t('ui.notify_critical_change.add_new_entities')}\n",
                    title=f"{TITLE} {i18n.t('ui.notify_critical_change.critical_entities_change')}",
                    persistent_notification_id= f"{__name__}_{func_name}_battmind_select_release_entity_update_required"
                )

def build_comment_db_yaml() -> dict:
    _LOGGER = globals()['_LOGGER'].getChild("build_comment_db_yaml")
    global COMMENT_DB_YAML
    
    for cfg_key, i18n_key in i18n.get_catalog().items():
        if not cfg_key.startswith("config."):
            continue
        
        cfg_key = cfg_key.removeprefix("config.")
        
        COMMENT_DB_YAML[cfg_key] = i18n.t(
            i18n_key,
            name=__name__,
        )
    
    return COMMENT_DB_YAML

def localize_default_entities():
    global DEFAULT_ENTITIES

    domains = ("input_button", "input_boolean", "input_number", "input_datetime")

    common_fields = ("name", "description", "unit_of_measurement")
    fmt = {"name": __name__}

    def _domain_and_suffix(domain_hint: str, key: str):
        if "." in key:
            dom, tail = key.split(".", 1)
        else:
            dom, tail = domain_hint, key

        prefix = f"{__name__}_"
        if tail.startswith(prefix):
            tail = tail[len(prefix):]
        return dom, tail

    def _localize_attrs(dom: str, key: str, attrs: dict,
                        fields: tuple = common_fields,
                        ignore_fields: tuple = ()):
        _, suf = _domain_and_suffix(dom, key)
        base = f"entity.{dom}.{suf}"

        if isinstance(fields, str):
            fields = (fields,)
        if isinstance(ignore_fields, str):
            ignore_fields = (ignore_fields,)

        for field in fields:
            if field in ignore_fields:
                continue
            if field not in attrs:
                val = i18n.t(f"{base}.{field}", default="", **fmt)
                if val:
                    attrs[field] = val

        for field, current in list(attrs.items()):
            if not isinstance(current, str):
                continue
            if field in ignore_fields:
                continue
            val = i18n.t(f"{base}.{field}", default=current, **fmt)
            if val and val != current:
                attrs[field] = val

    def _visit_entities(domain: str, entities, fn):
        if domain == "homeassistant" and isinstance(entities, dict):
            customize = entities.get("customize", {})
            if isinstance(customize, dict):
                for full_eid, attrs in customize.items():
                    if not isinstance(attrs, dict):
                        continue
                    eff_dom, _ = _domain_and_suffix("unknown", full_eid)
                    
                    fn(
                        eff_dom,
                        full_eid,
                        attrs,
                        fields=("description",),
                        ignore_fields=(),
                    )

        if domain in domains and isinstance(entities, dict):
            for key, attrs in entities.items():
                if not isinstance(attrs, dict):
                    continue
                fn(
                    domain,
                    key,
                    attrs,
                    fields=("name", "unit_of_measurement"),
                    ignore_fields=("description",),
                )

        if domain == "template" and isinstance(entities, list):
            supported_domains = (
                "binary_sensor",
                "sensor",
                "switch",
                "number",
                "select",
            )

            for block in entities:
                if not isinstance(block, dict):
                    continue

                for tmpl_domain in supported_domains:
                    ent_list = block.get(tmpl_domain)
                    if not isinstance(ent_list, list):
                        continue

                    for ent in ent_list:
                        if not isinstance(ent, dict):
                            continue

                        key = (
                            ent.get("default_entity_id")
                            or ent.get("entity_id")
                            or ent.get("name")
                        )
                        if not key:
                            continue

                        fn(
                            tmpl_domain,
                            key,
                            ent,
                            fields=("name", "unit_of_measurement"),
                            ignore_fields=("description"),
                        )

    for domain, entities in DEFAULT_ENTITIES.items():
        try:
            _visit_entities(domain, entities, _localize_attrs)
        except Exception as e:
            _LOGGER.error(
                f"Error localizing default entities for domain '{domain}': {e} {type(e)}"
            )

def load_language():
    func_name = "load_language"
    func_prefix = f"{func_name}_"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    
    try:
        TASKS[f"{func_prefix}load_catalog"] = task.create(i18n.load_catalog, "pyscript/modules/battmind_translations/")
        done, pending = task.wait({TASKS[f"{func_prefix}load_catalog"]})
        
        TASKS[f"{func_prefix}set_lang"] = task.create(i18n.set_lang, "en-GB")
        done, pending = task.wait({TASKS[f"{func_prefix}set_lang"]})
        
        _LOGGER.info(f"{BASENAME} i18n set language to: {i18n.get_lang()}")
        _LOGGER.info(f"{BASENAME} i18n get_available_langs: {i18n.get_available_langs()}")
        #_LOGGER.info(f"{BASENAME} loaded i18n catalog: {pformat(i18n.get_catalog(), width=200, compact=True)}")
    except (asyncio.CancelledError, asyncio.TimeoutError, KeyError) as e:
        _LOGGER.warning(f"Task cancelled or timeout: {e} {type(e)}")
        return
    except Exception as e:
        _LOGGER.error(f"Error in {func_name}: {e} {type(e)}")
        my_persistent_notification(
            f"Error in {func_name}: {e} {type(e)}",
            title=f"{TITLE} error",
            persistent_notification_id=f"{__name__}_{func_name}_error"
        )
    finally:
        task_cancel(func_prefix, task_remove=True, startswith=True)

def validate_config(config: dict, default: dict, path: str = "") -> bool:
    """
    Recursively validate that CONFIG values match the type of DEFAULT_CONFIG.
    Missing keys in CONFIG are ignored (not treated as errors).
    
    Args:
        config (dict): The CONFIG dict to validate.
        default (dict): The DEFAULT_CONFIG dict to compare against.
        path (str): Internal path for error messages.

    Returns:
        bool: True if valid, False otherwise.
    """
    func_name = "validate_config"
    _LOGGER = globals()["_LOGGER"].getChild(func_name)
    
    valid = True

    for key, default_val in default.items():
        full_path = f"{path}.{key}" if path else key

        if key not in config:
            continue

        cfg_val = config[key]

        if isinstance(default_val, dict):
            if not isinstance(cfg_val, dict):
                _LOGGER.error(f"Type mismatch at {full_path}: expected dict, got {type(cfg_val).__name__}")
                valid = False
            else:
                if not validate_config(cfg_val, default_val, full_path):
                    valid = False
            continue

        if isinstance(default_val, list):
            if not isinstance(cfg_val, list):
                _LOGGER.error(f"Type mismatch at {full_path}: expected list, got {type(cfg_val).__name__}")
                valid = False
            elif default_val and cfg_val:
                exp_type = type(default_val[0])
                for idx, val in enumerate(cfg_val):
                    if not isinstance(val, exp_type):
                        _LOGGER.error(
                            f"Type mismatch at {full_path}[{idx}]: expected {exp_type.__name__}, got {type(val).__name__}"
                        )
                        valid = False
            continue

        if not isinstance(cfg_val, type(default_val)):
            if isinstance(default_val, float) and isinstance(cfg_val, int):
                continue
            _LOGGER.error(f"Type mismatch at {full_path}: expected {type(default_val).__name__}, got {type(cfg_val).__name__}")
            valid = False

    return valid

def validate_entities(config: dict, path: str = "") -> tuple[list[str], list[str]]:
    """
    Validate that all configured entity_ids exist and are available in Home Assistant.

    Args:
        config (dict): The CONFIG dictionary.
        path (str): Internal path for recursion (optional).

    Returns:
        tuple[list[str], list[str]]:
            - missing: list of entity_ids not found at all
            - unavailable: list of entity_ids found but state is unavailable
    """
    _LOGGER = globals()["_LOGGER"].getChild("validate_entities")
    missing: list[str] = []
    unavailable: list[str] = []

    for key, value in config.items():
        full_path = f"{path}.{key}" if path else key

        if key == "entity_ids" and isinstance(value, dict):
            for eid_key, eid_val in value.items():
                if isinstance(eid_val, str) and eid_val:
                    if "." not in eid_val:
                        _LOGGER.error(f"Invalid entity_id format at {full_path}.{eid_key}: {eid_val}")
                        missing.append(eid_val)
                        continue
                    domain = eid_val.split(".")[0]
                    known_entities = state.names(domain=domain)
                    if eid_val not in known_entities:
                        missing.append(eid_val)
                    else:
                        st = get_state(eid_val, try_history=False, error_state=None)
                        _LOGGER.debug(f"Entity {eid_val} state: {st}")
                        if st in ENTITY_UNAVAILABLE_STATES:
                            unavailable.append(eid_val)

                elif isinstance(eid_val, list):
                    for idx, entity in enumerate(eid_val):
                        if not entity:
                            continue
                        if "." not in entity:
                            _LOGGER.error(f"Invalid entity_id format at {full_path}.{eid_key}[{idx}]: {entity}")
                            missing.append(entity)
                            continue
                        domain = entity.split(".")[0]
                        known_entities = state.names(domain=domain)
                        if entity not in known_entities:
                            missing.append(entity)
                        else:
                            _LOGGER.debug(f"Entity {entity} state: {get_state(entity)}")
                            st = get_state(entity, try_history=False, error_state=None)
                            if st in ENTITY_UNAVAILABLE_STATES:
                                unavailable.append(entity)

        elif isinstance(value, dict):
            m, u = validate_entities(value, full_path)
            missing.extend(m)
            unavailable.extend(u)

    return missing, unavailable

def validation_procedure():
    func_name = "validation_procedure"
    func_prefix = f"{func_name}_"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    global CONFIG
    
    task.wait_until(timeout=0.5)
    set_charging_rule(f"📟{i18n.t('ui.validation_procedure.entity_validation')}")
    missing_entity_ids, unavailable_entity_ids = validate_entities(CONFIG)
    if missing_entity_ids:
        _LOGGER.warning(f"Some configured entity IDs are missing, checking again in 2 min: {missing_entity_ids}")
        set_charging_rule(f"📟{i18n.t('ui.validation_procedure.entity_missing_try_again')}")
        task.wait_until(timeout=120)
        
        missing_entity_ids, unavailable_entity_ids = validate_entities(CONFIG)
        if missing_entity_ids:
            entity_ids = '\n- '.join(missing_entity_ids)
            my_persistent_notification(
                f"- {entity_ids}\n\n"
                f"{i18n.t('ui.validation_procedure.check_entity_ids')}",
                title=f"{TITLE} {i18n.t('ui.validation_procedure.missing_entity_ids_title')}",
                persistent_notification_id=f"{__name__}_{func_name}_missing_entity_ids"
            )
            shutdown()
    _LOGGER.info(f"All configured entity IDs are present.")
        
    if unavailable_entity_ids:
        _LOGGER.warning(f"Some configured entity IDs are unavailable, checking again in 2 min: {unavailable_entity_ids}")
        set_charging_rule(f"📟{i18n.t('ui.validation_procedure.entity_unavailable_try_again')}")
        
        task_set = set()
        try:
            for entity_id in unavailable_entity_ids:
                
                task_name = f"{func_prefix}_{entity_id}"
                TASKS[task_name] = task.create(wait_for_entity_update, entity_id=entity_id, updated_within_minutes=2, max_wait_time_minutes=2, check_interval=5.0)
                TASKS[task_name].set_name(task_name)
                task_set.add(TASKS[task_name])
                
            done, pending = task.wait(task_set)
        except (asyncio.CancelledError, asyncio.TimeoutError, KeyError) as e:
            _LOGGER.warning(f"Task was cancelled or timed out in {func_name}: {e} {type(e)}")
        except Exception as e:
            _LOGGER.error(f"Error in {func_name}: {e} {type(e)}")
            my_persistent_notification(
                f"Error in {func_name}: {e} {type(e)}",
                title=f"{TITLE} error",
                persistent_notification_id=f"{__name__}_{func_name}_error"
            )
        finally:
            task_cancel(func_prefix, startswith=True)
        
        _, unavailable_entity_ids = validate_entities(CONFIG)
        if unavailable_entity_ids:
            entity_ids = '\n- '.join(unavailable_entity_ids)
            my_persistent_notification(
                f"- {entity_ids}\n\n"
                f"{i18n.t('ui.validation_procedure.check_entity_ids')}",
                title=f"{TITLE} {i18n.t('ui.validation_procedure.unavailable_entity_ids_title')}",
                persistent_notification_id=f"{__name__}_{func_name}_unavailable_entity_ids"
            )
    _LOGGER.info(f"All configured entity IDs are available.")
        
    set_charging_rule(f"📟{i18n.t('ui.init.script_starting')}")

def init():
    func_name = "init"
    func_prefix = f"{func_name}_"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    global CONFIG, CONFIG_LAST_MODIFIED, DEFAULT_ENTITIES, INITIALIZATION_COMPLETE, COMMENT_DB_YAML, TESTING

    def handle_yaml(file_path, default_content, key_renaming, comment_db, check_nested_keys=False, check_first_run=False, prompt_restart=False):
        """
        Handles the loading, updating, and saving of YAML configurations, and optionally prompts for a restart.
        """
        fmt = {"file_path": file_path}
        
        if not file_exists(file_path):
            TASKS[f'{func_prefix}not_exists_save_yaml_{file_path}'] = task.create(save_yaml, file_path, default_content, comment_db)
            done, pending = task.wait({TASKS[f'{func_prefix}not_exists_save_yaml_{file_path}']})
    
            _LOGGER.info(f"File has been created: {file_path}")
            if "config.yaml" in file_path:
                my_persistent_notification(
                    f"{i18n.t('ui.init.file_created_message', **fmt)}\n\n"
                    f"{i18n.t('ui.init.add_entities')}",
                    title=f"{TITLE} ",
                    persistent_notification_id=f"{__name__}_{func_name}_{file_path}_created"
                )
            else:
                my_persistent_notification(
                    f"{i18n.t('ui.init.yaml_file_created', **fmt)}\n\n"
                    f"{i18n.t('ui.init.restart_required')}",
                    title=f"{TITLE}",
                    persistent_notification_id=f"{__name__}_{func_name}_{file_path}_created"
                )
            
            raise Exception(f"Edit it as needed. Please restart Home Assistant after making necessary changes.")
        
        TASKS[f'{func_prefix}load_yaml_{file_path}'] = task.create(load_yaml, file_path)
        done, pending = task.wait({TASKS[f'{func_prefix}load_yaml_{file_path}']})
        content = TASKS[f'{func_prefix}load_yaml_{file_path}'].result()
        
        _LOGGER.debug(f"Loaded content from {file_path}:\n{pformat(content, width=200, compact=True)}")

        if not content:
            set_charging_rule(f"📟{i18n.t('ui.init.error_loading', **fmt)}")
            _LOGGER.warning(f"Content of {file_path} is empty, reloading it")
            
            task.wait_until(timeout=5.0)
            
            TASKS[f'{func_prefix}load_yaml_{file_path}'] = task.create(load_yaml, file_path)
            done, pending = task.wait({TASKS[f'{func_prefix}load_yaml_{file_path}']})
            content = TASKS[f'{func_prefix}load_yaml_{file_path}'].result()
            
            if not content:
                raise Exception(f"Failed to load {file_path}")
        
        updated = False
        
        notify_critical_change(cfg = content, filename = file_path)
        
        if "config.yaml" in file_path:
            i18n.set_lang(content.get("language", 'en-GB'))
            comment_db = build_comment_db_yaml()
            updated, content = update_dict_with_new_keys(content, default_content)
        else:
            if not dicts_equal(content, default_content):
                updated = True
                content = default_content
        
        if content == {}:
            raise Exception(f"{file_path} is empty after updating keys, removing file")
        
        if (updated or file_path == f"{__name__}_config.yaml"):
            TASKS[f'{func_prefix}updated_save_yaml_{file_path}'] = task.create(save_yaml, file_path, content, comment_db)
            done, pending = task.wait({TASKS[f'{func_prefix}updated_save_yaml_{file_path}']})
            
        old_content = deepcopy(content)
        
        if key_renaming:
            is_config_file = True if "config.yaml" in file_path else False
            
            keys_renamed = []
            keys_renamed_log = []
            all_entities = state.names() if not is_config_file else []
            
            content = rename_dict_keys(content, key_renaming, remove_old_keys=False)
            
            for old_path, new_path in key_renaming.items():
                if is_config_file:
                    if get_dict_value_with_path(content, old_path) is None:
                        continue
                    
                    content = delete_dict_key_with_path(content, old_path)
                    keys_renamed.append(f"{old_path} -> {new_path}")
                    keys_renamed_log.append(f"Renamed {old_path} to {new_path}")
                else:
                    old_entity_id = ".".join(old_path.split(".")[-2:])
                    new_entity_id = ".".join(new_path.split(".")[-2:])
                    if old_entity_id in all_entities and new_entity_id not in all_entities:
                        old_entity_id_state = get_state(old_entity_id)
                        old_entity_id_attr = get_attr(old_entity_id, error_state={})
                        
                        if new_entity_id in all_entities:
                            if not is_entity_available(new_entity_id):
                                set_state(new_entity_id, old_entity_id_state)
                                
                            new_entity_id_state = get_state(new_entity_id)
                            if old_entity_id_state == new_entity_id_state or (is_entity_available(new_entity_id) and "restored" in old_entity_id_attr and old_entity_id_attr["restored"] == True):
                                content = delete_dict_key_with_path(content, old_path)
                                keys_renamed.append(f"{old_path} ({old_entity_id_state}) ({i18n.t('ui.init.renamed_to')}) {new_path} ({new_entity_id_state})")
                                keys_renamed_log.append(f"Renamed {old_path} ({old_entity_id_state}) (renamed) to {new_path} ({new_entity_id_state})")
                            else:
                                keys_renamed.append(f"{old_path} ({old_entity_id_state}) -> {new_path} ({new_entity_id_state})")
                                keys_renamed_log.append(f"Renamed {old_path} ({old_entity_id_state}) to {new_path} ({new_entity_id_state})")
                        else:
                            keys_renamed.append(f"{old_path} ({old_entity_id_state}) -> {new_path} ({i18n.t('ui.init.created')})")
                            keys_renamed_log.append(f"Renamed {old_path} ({old_entity_id_state}) to {new_path} (Created)")
                            
            if content == {}:
                raise Exception(f"{file_path} is empty after renaming keys, removing file")
                
            if old_content != content:
                for log_string in keys_renamed_log:
                    _LOGGER.info(log_string)
                
                config_entity_title = i18n.t('ui.init.config_renamed_keys', file_path=file_path) if "config.yaml" in file_path else i18n.t('ui.init.config_entities_renamed', file_path=file_path)
                my_persistent_notification(
                    message = f"{'\n'.join(keys_renamed)}",
                    title=f"{TITLE} {config_entity_title}",
                    persistent_notification_id=f"{__name__}_{func_name}_{file_path}_renaming_keys"
                )
                
                TASKS[f'{func_prefix}key_moved_save_yaml_{file_path}'] = task.create(save_yaml, file_path, content, comment_db)
                done, pending = task.wait({TASKS[f'{func_prefix}key_moved_save_yaml_{file_path}']})
        
        deprecated_keys = compare_dicts_unique_to_dict1(content, default_content)
        
        deprecated_keys = {
            k: v
            for k, v in deprecated_keys.items()
            if "homeassistant.customize" not in k
        }
        
        if deprecated_keys:
            if "config.yaml" in file_path:
                _LOGGER.info(f"Removing deprecated keys from {file_path}:")
                for key, value in deprecated_keys.items():
                    _LOGGER.info(f"\tRemoving deprecated key: {key}")
                    content = delete_flattened_key(content, key)
            else:
                _LOGGER.warning(f"{file_path} contains deprecated settings:")
                for key, value in deprecated_keys.items():
                    _LOGGER.warning(f"\t{key}: {value}")
                _LOGGER.warning("Please remove them.")
                my_persistent_notification(
                    f"{i18n.t('ui.init.deprecated_keys_in', file_path=file_path)}\n"
                    f"{i18n.t('ui.init.remove_these_keys')}:\n"
                    f"{'\n'.join(deprecated_keys.keys())}",
                    title=f"{TITLE} {i18n.t('ui.init.deprecated_keys_title')}",
                    persistent_notification_id=f"{__name__}_{func_name}_{file_path}_deprecated_keys"
                )
            
        if default_content != content and "packages" in file_path and not updated:
            content = default_content
            TASKS[f'{func_prefix}packages_changed_save_yaml_{file_path}'] = task.create(save_yaml, file_path, content, comment_db)
            done, pending = task.wait({TASKS[f'{func_prefix}packages_changed_save_yaml_{file_path}']})
            raise Exception(i18n.t('ui.init.restart_required'))
            
        if updated:
            msg = i18n.t('ui.init.config_updated', file_path=file_path) if "config.yaml" in file_path else i18n.t('ui.init.entities_package_updated', file_path=file_path)
            
            if check_first_run:
                msg += f"\n{i18n.t('ui.init.first_run_warning', file_path=file_path)}"
            msg += f"\n{i18n.t('ui.init.restart_required')}"
            
            if ("first_run" in content and content['first_run']) or "config.yaml" not in file_path:
                raise Exception(msg)

        if prompt_restart and updated:
            raise Exception(i18n.t('ui.init.restart_required'))

        return content
    
    set_charging_rule(f"📟{i18n.t('ui.init.script_starting')}")
    welcome_text = welcome()
    _LOGGER.info("-" * len(welcome_text))
    _LOGGER.info(welcome_text)
    _LOGGER.info("-" * len(welcome_text))
    task.wait_until(timeout=1.0)
    try:
        set_charging_rule(f"📟{i18n.t('ui.init.loading_config')}")
        
        CONFIG = handle_yaml(f"{__name__}_config.yaml", deepcopy(DEFAULT_CONFIG), deepcopy(CONFIG_KEYS_RENAMING), deepcopy(COMMENT_DB_YAML), check_first_run=True, prompt_restart=False)
        CONFIG_LAST_MODIFIED = get_file_modification_time(f"{__name__}_config.yaml")
        
        task.wait_until(timeout=0.5)
        set_charging_rule(f"📟{i18n.t('ui.init.config_validation')}")
        
        if not validate_config(CONFIG, DEFAULT_CONFIG):
            raise Exception(i18n.t('ui.init.config_validation_failed'))
        
        _LOGGER.info(f"{BASENAME} CONFIG loaded and validated")
        
        TESTING = True if "test" in __name__ or ("testing_mode" in CONFIG and CONFIG['testing_mode']) else False
        
        for entity_type in ['input_boolean', 'input_number']:
            DEFAULT_ENTITIES[entity_type] = {
                key: value for key, value in DEFAULT_ENTITIES[entity_type].items() if "preheat" not in key or "public_charging_session" not in key
            }
            
        for i, domain in enumerate(DEFAULT_ENTITIES['template']):
            if not isinstance(domain, dict):
                continue
            
            if "binary_sensor" in domain:
                binary_sensor_list = domain['binary_sensor']
                
                if not isinstance(binary_sensor_list, list):
                    continue
                
                for n, entity_dict in enumerate(binary_sensor_list):
                    if not isinstance(entity_dict, dict):
                        continue
                    
                    if "public_charging_session" in entity_dict.get('default_entity_id', ''):
                        DEFAULT_ENTITIES['template'][i]['binary_sensor'].pop(n)
            
            if "sensor" in domain:
                sensor_list = domain['sensor']
                
                if not isinstance(sensor_list, list):
                    continue
        
        if not is_powerwall_configured(CONFIG) and not CONFIG['first_run']:
            raise Exception(i18n.t('ui.init.config_powerwall_required'))
            
        if not is_solar_configured(CONFIG):
            keys_to_remove = [
                f"{__name__}_kwh_charged_by_solar",
                f"{__name__}_solar_sell_fixed_price"
            ]

            for key in keys_to_remove:
                DEFAULT_ENTITIES.get('input_boolean', {}).pop(key, None)
                DEFAULT_ENTITIES.get('input_number', {}).pop(key, None)

            for i, domain in enumerate(DEFAULT_ENTITIES['template']):
                if not isinstance(domain, dict):
                    continue
                
                if "sensor" in domain:
                    sensor_list = domain['sensor']
                    
                    if not isinstance(sensor_list, list):
                        continue
                    
                    for n, entity_dict in enumerate(sensor_list):
                        if not isinstance(entity_dict, dict):
                            continue
                        
                        if "solar" in entity_dict.get('default_entity_id', ''):
                            DEFAULT_ENTITIES['template'][i]['sensor'].pop(n)
                
        if __name__ != "battmind" and "input_select.battmind_select_release" in state.names(domain="input_select"):
            DEFAULT_ENTITIES.get('input_select', {}).pop("battmind_select_release", None)
        
        localize_default_entities()
        
        handle_yaml(f"packages/{__name__}.yaml", deepcopy(DEFAULT_ENTITIES), deepcopy(ENTITIES_RENAMING), None, check_nested_keys=True, prompt_restart=True)

        if CONFIG['first_run']:
            raise Exception(i18n.t('ui.init.first_run_warning', file_path=f"{__name__}_config.yaml"))
                
        INITIALIZATION_COMPLETE = True
    except (asyncio.CancelledError, asyncio.TimeoutError, KeyError) as e:
        _LOGGER.warning(f"Task cancelled or timeout: {e} {type(e)}")
        return
    except Exception as e:
        _LOGGER.error(e)
        INITIALIZATION_COMPLETE = False
        set_charging_rule(f"⛔{i18n.t('ui.init.script_stopped')}.\n{i18n.t('ui.init.check_log')}:\n{e} {type(e)}")
        my_persistent_notification(
            f"{i18n.t('ui.init.script_stopped')}\n"
            f"{i18n.t('ui.init.check_log')}:\n"
            f"{e}\n",
            title=f"{TITLE} {i18n.t('ui.init.script_stopped')}",
            persistent_notification_id=f"{__name__}_{func_name}_error"
        )
    finally:
        task_cancel(func_prefix, task_remove=True, startswith=True)

@service(f"pyscript.{__name__}_all_entities")
def get_all_entities(trigger_type=None, trigger_id=None, **kwargs):
    """yaml
    name: "BattMind: Get All Entities"
    description: "Get a list of all entities created by BattMind, along with their descriptions if available. Useful for debugging and documentation purposes."
    """
    func_name = "get_all_entities"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    global DEFAULT_ENTITIES
    entities = []
    yaml_card = ["type: vertical-stack\n", "cards:\n"]
    
    for domain_name, sub_dict in DEFAULT_ENTITIES.items():
        if "homeassistant" in domain_name:
            continue
        
        if domain_name == "binary_sensor":
            yaml_card.append(f"  - type: entities\n    title: 🎚️ {i18n.t('ui.get_all_entities.binary_sensors')}\n    state_color: true\n    entities:\n")
            for sensor_dict in sub_dict:
                for entity_name in sensor_dict["sensors"].keys():
                    yaml_card.append(f"    - {domain_name}.{entity_name}\n")
                    entities.append(f"{domain_name}.{entity_name}")
        
        elif domain_name == "sensor":
            yaml_card.append(f"  - type: entities\n    title: 📊 {i18n.t('ui.get_all_entities.sensors')}\n    state_color: true\n    entities:\n")
            for sensor_dict in sub_dict:
                for entity_name in sensor_dict["sensors"].keys():
                    yaml_card.append(f"    - {domain_name}.{entity_name}\n")
                    entities.append(f"{domain_name}.{entity_name}")
        
        else:
            yaml_card.append(f"  - type: entities\n    title: 📦 {domain_name.capitalize()}\n    state_color: true\n    entities:\n")
            for entity_name in sub_dict.keys():
                yaml_card.append(f"    - {domain_name}.{entity_name}\n")
                entities.append(f"{domain_name}.{entity_name}")
    
    yaml_card.append("  - type: markdown\n    content: >-\n")
    for entity in entities:
        description = DEFAULT_ENTITIES.get("homeassistant", {}).get('customize', {}).get(entity, {}).get('description', '')
        description = f"<br>{description}<br><br>" if description else ""
        yaml_card.append(f"      - <b>`{entity}`</b>{description}\n")
        
    notify_message = [f"## {i18n.t('ui.get_all_entities.all_entities')}:\n\n"]
    for entity in entities:
        description = DEFAULT_ENTITIES.get("homeassistant", {}).get('customize', {}).get(entity, {}).get('description', '')
        description = f"\n{description}\n" if description else ""
        notify_message.append(f"- <b>`{entity}`</b>{description}\n")
        
    my_persistent_notification(
        message = "\n".join(notify_message),
        title=f"{TITLE} {i18n.t('ui.get_all_entities.all_entities')}",
        persistent_notification_id=f"{__name__}_{func_name}"
    )
        
    if trigger_type == "service":
        return
    
    _LOGGER.info(f"Entities:\n{"".join(yaml_card)}")
    
    return entities

load_language()
init()

if INITIALIZATION_COMPLETE:
    is_solar_configured()
    is_powerwall_configured()
    
    MAX_KWH_CHARGING = CONFIG['solar']['powerwall_charging_power_limit'] / 1000
    MAX_WATT_CHARGING = CONFIG['solar']['powerwall_charging_power_limit']

def set_entity_friendlynames():
    func_name = "set_entity_friendlynames"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)

    for key, entry in CHARGING_TYPES.items():
        if "entity_name" in entry:
            name = f"{entry['emoji']} {i18n.t(f'charging_type.{key}.description', default='No description')}"
            _LOGGER.info(f"Setting sensor.{entry['entity_name']}.name: {name}")
            set_attr(f"sensor.{entry['entity_name']}.name", name)

def emoji_description():
    func_name = "emoji_description"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)

    items_sorted = sorted(CHARGING_TYPES.items(), key=lambda kv: float(kv[1].get('priority', 0)))

    lines = [
        f"* **{entry.get('emoji', '❓')} "
        f"{i18n.t(f'charging_type.{key}.description', default='No description')}**"
        for key, entry in items_sorted
    ]
    descriptions = [f"## {i18n.t('ui.emoji_description.header')} ##"] + lines

    _LOGGER.info(f"Setting sensor.{__name__}_emoji_description")

    md_hint = i18n.t('ui.emoji_description.markdown_hint', name=__name__)

    set_state(f"sensor.{__name__}_emoji_description", md_hint)
    set_attr(f"sensor.{__name__}_emoji_description.description", "\n".join(descriptions))

def emoji_sorting(text):
    _LOGGER = globals()['_LOGGER'].getChild("emoji_sorting")
    emoji_to_priority = {d['emoji']: float(d['priority']) for _, d in CHARGING_TYPES.items()}
    emoji_sorted = {}
    for emoji in text.split():
        if emoji in emoji_to_priority:
            emoji_sorted[emoji_to_priority[emoji]] = emoji
    
    emojis = [emoji for _, emoji in sorted(emoji_sorted.items())]
    return " ".join(emojis)

def emoji_parse(data):
    emojis = [CHARGING_TYPES[key]['emoji'] for key in data if data[key] is True and key in CHARGING_TYPES]
    return emoji_sorting(" ".join(emojis))

def join_unique_emojis(str1: str, str2: str) -> str:
    _LOGGER = globals()['_LOGGER'].getChild("join_unique_emojis")
    if str1 and str2:
        emojis1 = set(str1.split())
        emojis2 = set(str2.split())
        unique_emojis = emojis1.union(emojis2)
        _LOGGER.info(f"Joining emojis: {str1} + {str2} -> {' '.join(unique_emojis)}")
        return ' '.join(unique_emojis)
    return str1 or str2

def emoji_text_format(text, group_size=3):
    _LOGGER = globals()['_LOGGER'].getChild("emoji_text_format")
    words = text.split()
    _LOGGER.info(f"Formatting emojis: {text} into groups of {group_size}")

    grouped_text = []
    string_builder = []
    for word in words:
        string_builder.append(word)
        if len(string_builder) == group_size:
            grouped_text.append(' '.join(string_builder))
            string_builder = []
    
    if len(words) <= group_size:
        return ''.join(words)

    grouped_text = [''.join(words[i:i+group_size]) for i in range(0, len(words), group_size)]
    
    return '<br>'.join(grouped_text)

def emoji_update_local_energy(emojis=[], kwh_from_local_energy=0.0, solar_kwh_of_local_energy=0.0, powerwall_kwh_of_local_energy=0.0):
    if solar_kwh_of_local_energy > 0.0:
        emojis = join_unique_emojis(emojis, emoji_parse({'solar': True}))
    elif len(emojis.split(" ")) > 1 and solar_kwh_of_local_energy <= 0.0:
        emojis_set = set(emojis.split(" "))
        emojis_set.discard(emoji_parse({'solar': True}))
        emojis = " ".join(emojis_set)
        
    if powerwall_kwh_of_local_energy > 0.0:
        emojis = join_unique_emojis(emojis, emoji_parse({'powerwall': True}))
    elif len(emojis.split(" ")) > 1 and powerwall_kwh_of_local_energy <= 0.0:
        emojis_set = set(emojis.split(" "))
        emojis_set.discard(emoji_parse({'powerwall': True}))
        emojis = " ".join(emojis_set)
        
    return emojis

def set_default_entity_states():
    func_name = "set_default_entity_states"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    
    set_state(f"sensor.{__name__}_overview", i18n.t('ui.set_default_entity_states.markdown_guide', name=__name__))
    set_attr(f"sensor.{__name__}_overview.overview", f"<center>\n\n**{i18n.t('ui.set_default_entity_states.no_overview')}**\n\n</center>")
    
    entity_dict = {
        f"input_number.{__name__}_solar_sell_fixed_price": CONFIG['solar']['production_price'],
        f"input_number.{__name__}_cheap_price_periods": CONFIG['prices']['cheap_price_periods'],
        f"input_number.{__name__}_cheap_price_period_rise_threshold": CONFIG['prices']['cheap_price_period_rise_threshold'],
        f"input_number.{__name__}_cheapest_price_rise_threshold": CONFIG['prices']['cheapest_price_rise_threshold'],
    }
    entity_dict = {}
    
    for entity_id, value in entity_dict.items():
        try:
            if not is_entity_configured(entity_id):
                continue
                        
            if not is_solar_configured() and "solar" in entity_id:
                continue
            
            if not is_entity_available(entity_id):
                raise Exception(f"Entity {entity_id} not available cant set state")

            if str(get_state(entity_id, float_type=False, error_state=None)) in ENTITY_UNAVAILABLE_STATES:
                _LOGGER.info(f"Setting {entity_id} to {value}")

            set_state(entity_id, value)
        except Exception as e:
            _LOGGER.error(f"Error setting {entity_id} to {value}: {e} {type(e)}")

def weather_values():
    output = []
    for condition in WEATHER_CONDITION_DICT.values():
        if condition not in output:
            output.append(condition)
    return output
    
def get_list_values(data):
    float_list = []
    for item in data:
        if isinstance(item, float):
            float_list.append(item)
        elif isinstance(item, (list, tuple)):
            if isinstance(item[1], float):
                float_list.append(item[1])
    return float_list

def get_battery_level():
    func_name = "get_battery_level"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    
    try:
        return float(get_state(CONFIG["home"]["entity_ids"]["powerwall_battery_level_entity_id"], float_type=True, error_state=None))
    except Exception as e:
        _LOGGER.warning(f"Cant get powerwall battery level from {CONFIG['home']['entity_ids']['powerwall_battery_level_entity_id']}, using default value 100.0: {e} {type(e)}")
        return 100.0

def get_cheap_price_periods():
    func_name = "get_cheap_price_periods"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    
    try:
        return int(get_state(f"input_number.{__name__}_cheap_price_periods", float_type=True, error_state=None))
    except Exception as e:
        try:
            _LOGGER.warning(f"Cant get cheap price periods from input_number.{__name__}_cheap_price_periods, using config data {CONFIG['prices']['cheap_price_periods']}: {e} {type(e)}")
            return int(CONFIG['prices']['cheap_price_periods'])
        except Exception as e:
            _LOGGER.error(f"Failed to get cheap price periods from config: {e} {type(e)}")
            return 2
    
def get_cheap_price_period_rise_threshold():
    func_name = "get_cheap_price_period_rise_threshold"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    
    try:
        return float(get_state(f"input_number.{__name__}_cheap_price_period_rise_threshold", float_type=True, error_state=None))
    except Exception as e:
        try:
            _LOGGER.warning(f"Cant get cheap price periods from input_number.{__name__}_cheap_price_period_rise_threshold, using config data {CONFIG['prices']['cheap_price_period_rise_threshold']}: {e} {type(e)}")
            return float(CONFIG['prices']['cheap_price_period_rise_threshold'])
        except Exception as e:
            _LOGGER.error(f"Failed to get cheap price period rise threshold from config: {e} {type(e)}")
            return 0.1
    
def get_cheapest_price_rise_threshold():
    func_name = "get_cheapest_price_rise_threshold"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    
    try:
        return float(get_state(f"input_number.{__name__}_cheapest_price_rise_threshold", float_type=True, error_state=None))
    except Exception as e:
        try:
            _LOGGER.warning(f"Cant get cheapest price rise threshold from input_number.{__name__}_cheapest_price_rise_threshold, using config data {CONFIG['prices']['cheapest_price_rise_threshold']}: {e} {type(e)}")
            return float(CONFIG['prices']['cheapest_price_rise_threshold'])
        except Exception as e:
            _LOGGER.error(f"Failed to get cheapest price rise threshold from config: {e} {type(e)}")
            return 0.5

def get_exclude_sell_hours():
    func_name = "get_exclude_hours"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    
    try:
        exclude_hours_str = get_state(f"input_text.{__name__}_exclude_sell_hours", error_state="")
        exclude_hours = [int(hour.strip()) for hour in exclude_hours_str.split(",") if hour.strip().isdigit()]
        return exclude_hours
    except Exception as e:
        _LOGGER.error(f"Failed to get exclude hours from input_text.{__name__}_exclude_sell_hours, using default empty list: {e} {type(e)}")
        return []

def get_min_profit_per_kwh():
    func_name = "get_min_profit_per_kwh"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    try:
        return float(get_state(f"input_number.{__name__}_min_profit_per_kwh", float_type=True, error_state=None))
    except Exception as e:
        _LOGGER.error(f"Failed to get min profit per kwh from input_number.{__name__}_min_profit_per_kwh, using default 0.0: {e} {type(e)}")
        return 0.0

def deactivate_script_enabled():
    if get_state(f"input_boolean.{__name__}_deactivate_script") == "on":
        return True
    
def consumption_forecast_type():
    forecast_type = get_state(f"input_select.{__name__}_consumption_forecast_type", error_state="ema").lower()
    
    if "ema" in forecast_type:
        return "ema"
    elif "average" in forecast_type:
        return "avg"
    elif "trend" in forecast_type:
        return "trend"
    else:
        return "ema"

def cheapest_hour_fill_planner_enabled():
    if get_state(f"input_boolean.{__name__}_cheapest_hour_fill_planner") == "on":
        return True
    
def most_expensive_planner_enabled():
    if get_state(f"input_boolean.{__name__}_most_expensive_planner") == "on":
        return True
    
def only_discharge_on_profit_enabled():
    if get_state(f"input_boolean.{__name__}_only_discharge_on_profit") == "on":
        return True
    
def prioritize_discharge_hours_by_energy_cost_enabled():
    if get_state(f"input_boolean.{__name__}_prioritize_discharge_hours_by_energy_cost") == "on":
        return True
    
def sell_excess_kwh_available_enabled():
    if get_state(f"input_boolean.{__name__}_sell_excess_kwh_available") == "on":
        return True
    
def get_tariffs(hour, day_of_week):
    func_name = "get_tariffs"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    
    try:
        if CONFIG['prices']['entity_ids']['power_prices_entity_id'] not in state.names(domain="sensor"):
            raise Exception(f"{CONFIG['prices']['entity_ids']['power_prices_entity_id']} not loaded")
        
        power_prices_attr = get_attr(CONFIG['prices']['entity_ids']['power_prices_entity_id'], error_state={})
        
        if "tariffs" not in power_prices_attr:
            raise Exception(f"tariffs not in {CONFIG['prices']['entity_ids']['power_prices_entity_id']}")
        
        attr = power_prices_attr["tariffs"]
        transmissions_nettarif = attr["additional_tariffs"]["transmissions_nettarif"]
        systemtarif = attr["additional_tariffs"]["systemtarif"]
        elafgift = attr["additional_tariffs"]["elafgift"]
        tariffs = attr["tariffs"][str(hour)]
        tariff_sum = sum([transmissions_nettarif, systemtarif, elafgift, tariffs])
        
        return {
            "transmissions_nettarif": transmissions_nettarif,
            "systemtarif": systemtarif,
            "elafgift": elafgift,
            "tariffs": tariffs,
            "tariff_sum": tariff_sum
        }
        
    except Exception as e:
        _LOGGER.debug(f"get_raw_price(hour = {hour}, day_of_week = {day_of_week}): {e} {type(e)}")
        return {
                "transmissions_nettarif": 0.0,
                "systemtarif": 0.0,
                "elafgift": 0.0,
                "tariffs": 0.0,
                "tariff_sum": 0.0
            }

def get_solar_sell_price(set_entity_attr=False, get_avg_offline_sell_price=False, timestamp=None):
    func_name = "get_solar_sell_price"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    
    if not is_solar_configured(): return 0.0
    
    day_of_week = getDayOfWeek()
    try:
        sell_price = float(get_state(f"input_number.{__name__}_solar_sell_fixed_price", float_type=True, error_state=CONFIG['solar']['production_price']))
        entity_price = sell_price
        
        if get_avg_offline_sell_price:
            if sell_price != -1.0:
                return sell_price
            
            location = sun.get_astral_location(hass)
            sunrise = location[0].sunrise(getTime()).replace(tzinfo=None).hour
            sunset = location[0].sunset(getTime()).replace(tzinfo=None).hour
            sell_price_list = []
            
            for hour in range(sunrise, sunset):
                sell_price_list.append(average(KWH_AVG_PRICES_DB['history_sell'][hour][day_of_week]))
                
            return average(sell_price_list)
        
        if CONFIG['prices']['entity_ids']['power_prices_entity_id'] not in state.names(domain="sensor"):
            raise Exception(f"{CONFIG['prices']['entity_ids']['power_prices_entity_id']} not loaded")
        
        price = get_state(CONFIG['prices']['entity_ids']['power_prices_entity_id'], float_type=True)
        
        tariff_dict = get_tariffs(getHour(), day_of_week)
        transmissions_nettarif = tariff_dict["transmissions_nettarif"]
        systemtarif = tariff_dict["systemtarif"]
        elafgift = tariff_dict["elafgift"]
        tariffs = tariff_dict["tariffs"]
        
        tariff_sum = tariff_dict["tariff_sum"]
        raw_price = price - tariff_sum
        
        energinets_network_tariff = SOLAR_SELL_TARIFF["energinets_network_tariff"]
        energinets_balance_tariff = SOLAR_SELL_TARIFF["energinets_balance_tariff"]
        solar_production_seller_cut = SOLAR_SELL_TARIFF["solar_production_seller_cut"]
        
        sell_tariffs = sum((solar_production_seller_cut, energinets_network_tariff, energinets_balance_tariff, transmissions_nettarif, systemtarif))
        solar_sell_price = raw_price - sell_tariffs
        
        if sell_price == -1.0:
            sell_price = round(solar_sell_price, 3)
        
        if set_entity_attr:
            LOCAL_ENERGY_PRICES['solar_kwh_price'] = {
                "price": round(price, 3),
                "transmissions_nettarif": round(transmissions_nettarif, 3),
                "systemtarif": round(systemtarif, 3),
                "elafgift": round(elafgift, 3),
                "tariffs": round(tariffs, 3),
                "tariff_sum": round(tariff_sum, 3),
                "raw_price": round(raw_price, 3),
                "sell_tariffs_overview": "",
                "transmissions_nettarif_": round(transmissions_nettarif, 3),
                "systemtarif_": round(systemtarif, 3),
                "energinets_network_tariff_": round(energinets_network_tariff, 4),
                "energinets_balance_tariff_": round(energinets_balance_tariff, 4),
                "solar_production_seller_cut_": round(solar_production_seller_cut, 4),
                "sell_tariffs": round(sell_tariffs, 3),
                "solar_sell_price": round(solar_sell_price, 3)
            }
            if entity_price >= 0.0:
                LOCAL_ENERGY_PRICES['solar_kwh_price']['fixed_sell_price'] = sell_price
    except Exception as e:
        sell_price = None
        using_text = "default"
        try:
            sell_price = float(get_state(f"input_number.{__name__}_solar_sell_fixed_price", float_type=True, error_state=CONFIG['solar']['production_price']))
            if sell_price == -1.0:
                sell_price = average(KWH_AVG_PRICES_DB['history_sell'][getHour()][day_of_week])
                using_text = "database average"
        except Exception as e:
            pass
        
        if sell_price is None:
            sell_price = max(CONFIG['solar']['production_price'], 0.0)
            
        _LOGGER.error(f"Cant get solar sell price using {using_text} {sell_price}: {e} {type(e)}")
        
    return sell_price

def get_refund():
    func_name = "get_refund"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    
    try:
        return abs(CONFIG['prices']['refund'])
    except Exception as e:
        _LOGGER.warning(f"Cant get refund, using default 0.0: {e} {type(e)}")
        return 0.0

def get_powerwall_kwh_price(kwh = None, timestamp=None): #TODO use predicted prices instead of historical prices for more accurate estimation
    func_name = "get_powerwall_kwh_price"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    global POWER_VALUES_DB, KWH_AVG_PRICES_DB, LOCAL_ENERGY_PRICES
    
    if kwh is None:
        try:
            kwh = percentage_to_kwh(get_battery_level(), include_charging_loss=True)
        except Exception as e:
            _LOGGER.warning(f"Cant get kWh from powerwall battery level, using default {CONFIG['solar']['powerwall_battery_size']}: {e} {type(e)}")
            kwh = CONFIG['solar']['powerwall_battery_size']
    min_battery_kwh = percentage_to_kwh(CONFIG['solar']['powerwall_battery_level_min'], include_charging_loss=True)
    kwh -= min_battery_kwh
    
    kwh = max(kwh, min_battery_kwh)

    if timestamp is None:
        timestamp = reset_time_to_hour()
    prices = get_hour_prices()
    solar_prices = get_hour_prices(sell_prices=True)
    
    powerwall_kwh = []
    powerwall_total_cost = []
    kwh_price_list = []
    
    try:
        #for i in range(CONFIG['database']['power_values_db_data_to_save']):
        for hour in range(0, 24):
            if sum(powerwall_kwh) > kwh:
                break
            
            loop_timestamp = timestamp - datetime.timedelta(hours=hour)
            loop_timestamp_end = loop_timestamp.replace(minute=59, second=59, microsecond=0)
            days_ago = (timestamp - loop_timestamp).days
            day_of_week = getDayOfWeek(loop_timestamp)
            
            data = power_values(loop_timestamp_end - datetime.timedelta(hours=1), loop_timestamp_end)
            
            power_consumption_without_all_exclusion = data["power_consumption_without_all_exclusion"] / 1000.0
            powerwall_charging_consumption = data["powerwall_charging_consumption"] / 1000.0
            solar_production = data["solar_production"] / 1000.0
            
            solar_available_production = max(solar_production - power_consumption_without_all_exclusion, 0.0)
            
            if powerwall_charging_consumption > 0.0 and sum(powerwall_kwh) < kwh:
                solar_production_share = min(solar_available_production, powerwall_charging_consumption) / powerwall_charging_consumption if powerwall_charging_consumption > 0 else 0.0
                grid_share = 1.0 - solar_production_share
                
                offline_price = sum(KWH_AVG_PRICES_DB['history'][hour][day_of_week]) / len(KWH_AVG_PRICES_DB['history'][hour][day_of_week])
                offline_sell_price = sum(KWH_AVG_PRICES_DB['history_sell'][hour][day_of_week]) / len(KWH_AVG_PRICES_DB['history_sell'][hour][day_of_week])
                grid_share_price = offline_price * grid_share
                solar_share_price = offline_sell_price * solar_production_share
                
                try:
                    if loop_timestamp in prices:
                        grid_share_price = prices.get(loop_timestamp, None) * grid_share
                    else:
                        _LOGGER.warning(f"Timestamp {loop_timestamp} not in prices, using offline price {offline_price} and grid share {grid_share}")
                        grid_share_price = KWH_AVG_PRICES_DB['history'][hour][day_of_week][days_ago] * grid_share
                except Exception as e:
                    _LOGGER.warning(f"Cant get grid share price from prices for timestamp {loop_timestamp}, using offline price {offline_price} and grid share {grid_share}: {e} {type(e)}")
                
                try:
                    if loop_timestamp in solar_prices:
                        solar_share_price = solar_prices.get(loop_timestamp, None) * solar_production_share
                    else:
                        _LOGGER.warning(f"Timestamp {loop_timestamp} not in solar_prices, using offline sell price {offline_sell_price} and solar production share {solar_production_share}")
                        solar_share_price = KWH_AVG_PRICES_DB['history_sell'][hour][day_of_week][days_ago] * solar_production_share
                except Exception as e:
                    _LOGGER.warning(f"Cant get solar share price from solar_prices for timestamp {loop_timestamp}, using offline sell price {offline_sell_price} and solar production share {solar_production_share}: {e} {type(e)}")
                    
                kwh_price = grid_share_price + solar_share_price
                kwh_price_list.append(round(kwh_price, 3))
                powerwall_kwh.append(round(powerwall_charging_consumption, 3))
                powerwall_total_cost.append(round(powerwall_charging_consumption * kwh_price, 3))
    except Exception as e:
        _LOGGER.error(f"Error getting powerwall kWh price: {e} {type(e)}")
        return get_solar_sell_price()
    
    if powerwall_kwh:
        LOCAL_ENERGY_PRICES['powerwall_kwh_price'] = {
            "powerwall_kwh": powerwall_kwh,
            "powerwall_total_cost": powerwall_total_cost,
            "kwh_price_list": kwh_price_list,
            "kr_per_kwh": round(sum(powerwall_total_cost) / sum(powerwall_kwh), 3) if sum(powerwall_kwh) > 0.0 else 0.0
        }
    
    powerwall_kwh = sum(powerwall_kwh)
    powerwall_total_cost = sum(powerwall_total_cost)
    
    return powerwall_total_cost / powerwall_kwh if powerwall_kwh > 0.0 else 0.0

def kwh_to_percentage(kwh, include_charging_loss=False):
    return kwh / CONFIG['solar']['powerwall_battery_size'] * 100

def percentage_to_kwh(percentage, include_charging_loss=False):
    return percentage * CONFIG['solar']['powerwall_battery_size'] / 100

def kwh_needed_for_charging(targetLevel=None, battery=None):
    func_name = "kwh_needed_for_charging"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)

    targetLevel = targetLevel if targetLevel is not None else CONFIG['solar']['powerwall_battery_level_min']
    battery = battery if battery is not None else get_battery_level()

    kwh = round(percentage_to_kwh(targetLevel - battery, include_charging_loss=True), 2)
    _LOGGER.debug(f"targetLevel:{targetLevel} battery:{battery} kwh:{kwh} without loss")

    return max(kwh, 0.0)

def load_charging_history():
    func_name = "load_charging_history"
    func_prefix = f"{func_name}_"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    global CHARGING_HISTORY_DB
    
    def rename_key_recursive(data, old_key, new_key):
        renamed = False

        if isinstance(data, dict):
            if old_key in data:
                data[new_key] = data.pop(old_key)
                renamed = True
            for k, v in data.items():
                nested, changed = rename_key_recursive(v, old_key, new_key)
                data[k] = nested
                renamed = renamed or changed

        elif isinstance(data, list):
            for i, item in enumerate(data):
                data[i], changed = rename_key_recursive(item, old_key, new_key)
                renamed = renamed or changed

        return data, renamed
    
    try:
        filename = f"{__name__}_charging_history_db"
        TASKS[f'{func_prefix}create_yaml'] = task.create(create_yaml, filename, db=CHARGING_HISTORY_DB)
        done, pending = task.wait({TASKS[f'{func_prefix}create_yaml']})
        
        TASKS[f'{func_prefix}load_yaml'] = task.create(load_yaml, filename)
        done, pending = task.wait({TASKS[f'{func_prefix}load_yaml']})
        CHARGING_HISTORY_DB = TASKS[f'{func_prefix}load_yaml'].result()
    except (asyncio.CancelledError, asyncio.TimeoutError, KeyError) as e:
        _LOGGER.warning(f"load_charging_history(): {e} {type(e)}")
        return
    except Exception as e:
        error_message = f"Cant load {__name__}_charging_history_db: {e} {type(e)}"
        _LOGGER.error(error_message)
        save_error_to_file(error_message, caller_function_name = f"{func_name}()")
        my_persistent_notification(error_message, f"{TITLE} error", persistent_notification_id=f"{__name__}_{func_name}_load_charging_history")
    finally:
        task_cancel(func_prefix, task_remove=True, startswith=True)
    
    if CHARGING_HISTORY_DB == {} or not CHARGING_HISTORY_DB:
        CHARGING_HISTORY_DB = {}
        save_charging_history()
    
    CHARGING_HISTORY_DB, renamed_kWh_solar = rename_key_recursive(CHARGING_HISTORY_DB, "kWh_solar", "kWh_from_local_energy")
    
    if renamed_kWh_solar:
        _LOGGER.info(f"Renamed key 'kWh_solar' to 'kWh_from_local_energy' in {__name__}_charging_history_db.yaml")
        save_charging_history()

    set_state(f"sensor.{__name__}_charging_history", i18n.t('ui.load_charging_history.default_state'))
    
    charging_history_combine_and_set(get_ending_byte_size=True if CHARGING_HISTORY_ENDING_BYTE_SIZE is None else False)
    charging_history_combine_and_set()
    
def save_charging_history():
    func_name = "save_charging_history"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    global CHARGING_HISTORY_DB
    
    if CHARGING_HISTORY_DB:
        now = getTime()
        
        try:
            for key in dict(sorted(CHARGING_HISTORY_DB.items(), key=lambda item: item[0], reverse=True)).keys():
                if monthsBetween(key, now, error_value=0) > CONFIG['database']['charging_history_db_data_to_save'] + 1:
                    _LOGGER.warning(f"Removing {key} from CHARGING_HISTORY_DB")
                    del CHARGING_HISTORY_DB[key]
                
            CHARGING_HISTORY_DB = dict(sorted(CHARGING_HISTORY_DB.items(), key=lambda item: item[0], reverse=True))
        
            save_changes(f"{__name__}_charging_history_db", CHARGING_HISTORY_DB)
        except Exception as e:
            error_message = f"Cant save {__name__}_charging_history_db: {e} {type(e)}"
            _LOGGER.error(error_message)
            save_error_to_file(error_message, caller_function_name = f"{func_name}()")
            my_persistent_notification(error_message, f"{TITLE} error", persistent_notification_id=f"{__name__}_{func_name}")

def charging_history_combine_and_set(get_ending_byte_size: bool = False):
    func_name = "charging_history_combine_and_set"
    _LOGGER = globals()["_LOGGER"].getChild(func_name)
    global CHARGING_HISTORY_DB, CHARGING_HISTORY_ENDING_BYTE_SIZE

    # --- Combine-after (requested) ---
    combine_after = 10  # start combining only AFTER this many newest blocks
    
    history = []
    combined_db = {}

    if not CHARGING_HISTORY_DB:
        total_history_bytes_size = len("\n".join(history).encode("utf-8"))
        CHARGING_HISTORY_ENDING_BYTE_SIZE = total_history_bytes_size + 1000
        if not get_ending_byte_size:
            set_attr(f"sensor.{__name__}_charging_history.history", "**Ingen lade historik**")
        return

    def _num(v, default=0.0) -> float:
        return float(v) if isinstance(v, (int, float)) else default

    def _round_or_none(v, decimals=3):
        return round(float(v), decimals) if isinstance(v, (int, float)) else None

    def _fmt(v, decimals=2, default="") -> str:
        return f"{v:.{decimals}f}" if isinstance(v, (int, float)) else default

    def _buy_color(s: str) -> str:
        return f"<font color='white'>{s}</font>"

    def _charge_color(s: str) -> str:
        return f"<font color='lightgreen'>{s}</font>"

    def _solar_color(s: str) -> str:
        return f"<font color='yellow'>{s}</font>"

    def _discharge_color(s: str) -> str:
        return f"<font color='darkorange'>{s}</font>"

    # --- HA attribute byte budget ---
    max_entity_attributes_bytes_size = 16384
    history_bytes_buffer = 0 if CHARGING_HISTORY_ENDING_BYTE_SIZE is None else CHARGING_HISTORY_ENDING_BYTE_SIZE
    max_history_bytes_size = max_entity_attributes_bytes_size - history_bytes_buffer

    current_history_bytes_size = 0

    def history_loop_append(line_or_lines):
        nonlocal current_history_bytes_size
        if isinstance(line_or_lines, list):
            lines = [str(x) for x in line_or_lines]
            history.extend(lines)
            current_history_bytes_size += len("\n".join(lines).encode("utf-8"))
        else:
            s = str(line_or_lines)
            history.append(s)
            current_history_bytes_size += len(s.encode("utf-8"))

    # Newest first
    sorted_db = sorted(CHARGING_HISTORY_DB.items(), key=lambda item: item[0], reverse=True)
    sorted_db_len = len(sorted_db)

    # --- UI folding ---
    length = 10
    sub_length = 50
    sub_details_count = 0
    details = False
    add_header = False
    history_too_big_announced = False
    current_history_counter = 0  # markdown row counter

    combined_block_counter = 0

    # Labels
    charge_label = f"{emoji_parse({'charging': True})}{i18n.t('ui.common.price')}<br>({i18n.t('ui.common.valuta_kwh')})"
    discharge_label = f"{emoji_parse({'discharging': True})}{i18n.t('ui.common.price')}<br>({i18n.t('ui.common.valuta_kwh')})"
    savings_label = f"{emoji_parse({'profit': True})}<br>({i18n.t('ui.common.valuta_kwh')})"

    header = (
        f"| {i18n.t('ui.common.time')} | %<br>({emoji_parse({'solar': True})}) | kWh<br>({emoji_parse({'solar': True})}) | "
        f"{_charge_color(charge_label)} | "
        f"{_discharge_color(discharge_label)} | "
        f"{savings_label} |"
    )
    align = "|:---|---:|---:|:---:|:---:|:---:|"

    history_loop_append("<center>\n")
    history_loop_append(header)
    history_loop_append(align)

    # --- Totals (month + total) ---
    total = {
        "charged_kwh": {"total": 0.0},
        "charged_cost": {"total": 0.0},
        "charged_local_kwh": {"total": 0.0},
        "charged_grid_kwh": {"total": 0.0},
        "charged_pct": {"total": 0.0},

        "discharged_kwh": {"total": 0.0},
        "discharged_cost": {"total": 0.0},
        "discharged_pct": {"total": 0.0},

        "savings": {"total": 0.0},
    }

    def _ensure_month(month):
        for k in total.keys():
            if month not in total[k]:
                total[k][month] = 0.0

    def _read_session(session: dict):
        prices = session.get("prices", {}) if isinstance(session.get("prices", {}), dict) else {}
        charged = session.get("charged", {}) if isinstance(session.get("charged", {}), dict) else {}
        discharged = session.get("discharged", {}) if isinstance(session.get("discharged", {}), dict) else {}

        c_kwh = _num(charged.get("kWh", 0.0))
        c_pct = _num(charged.get("percentage", 0.0))
        c_cost = _num(charged.get("cost", 0.0))
        c_local = _num(charged.get("kWh_from_local_energy", 0.0))
        c_grid = _num(charged.get("kWh_from_grid", max(c_kwh - c_local, 0.0)))

        d_kwh = _num(discharged.get("kWh", 0.0))
        d_pct = _num(discharged.get("percentage", 0.0))
        d_cost = _num(discharged.get("cost", 0.0))
        savings = discharged.get("savings", None)
        savings_num = float(savings) if isinstance(savings, (int, float)) else 0.0

        return prices, c_kwh, c_pct, c_cost, c_local, c_grid, d_kwh, d_pct, d_cost, savings_num

    def _can_combine(base_when: datetime.datetime, next_when: datetime.datetime) -> bool:
        """
        Your wish: when we DO combine, do it per-day => one row per day.
        We still keep your 'recent = hourly' behavior if not yet combining.
        But once combined_block_counter >= combine_after, we want daily buckets.
        """
        try:
            return daysBetween(base_when, next_when) == 0
        except Exception:
            return False

    # --- Main loop (manual index so we can skip combined entries) ---
    idx = 0
    while idx < sorted_db_len:
        when, session = sorted_db[idx]

        if not isinstance(when, datetime.datetime) or not isinstance(session, dict):
            idx += 1
            continue

        # read base
        prices, c_kwh, c_pct, c_cost, c_local, c_grid, d_kwh, d_pct, d_cost, savings_num = _read_session(session)

        # Skip truly empty
        if c_kwh <= 0.0 and d_kwh <= 0.0 and c_pct <= 0.0 and d_pct <= 0.0 and savings_num == 0.0:
            idx += 1
            continue

        # --- combine after X newest blocks ---
        started = when
        end_when = when           # newest timestamp in block (for display)
        used_prices = deepcopy(prices)      # keep newest prices in block
        total_prices = {
            "buy_price": [],
            "sell_price": [],
            "powerwall_kwh_price": [],
            "kwh_savings": [],
            "charge_price": [],
            "discharge_price": [],
        }

        if combined_block_counter >= combine_after:
            # daily combine: merge all same-day entries into one block
            j = idx + 1
            while j < sorted_db_len:
                next_when, next_session = sorted_db[j]
                if not isinstance(next_when, datetime.datetime) or not isinstance(next_session, dict):
                    break
                if not _can_combine(when, next_when):
                    break

                n_prices, nc_kwh, nc_pct, nc_cost, nc_local, nc_grid, nd_kwh, nd_pct, nd_cost, nsavings_num = _read_session(next_session)
                
                if n_prices.get("buy_price", None):
                    total_prices["buy_price"].append(n_prices.get("buy_price", None))
                    
                if n_prices.get("sell_price", None):
                    total_prices["sell_price"].append(n_prices.get("sell_price", None))
                    
                if n_prices.get("powerwall_kwh_price", None):
                    total_prices["powerwall_kwh_price"].append(n_prices.get("powerwall_kwh_price", None))
                    
                if n_prices.get("kwh_savings", None):
                    total_prices["kwh_savings"].append(n_prices.get("kwh_savings", None))
                    
                if n_prices.get("charge_price", None):
                    total_prices["charge_price"].append(n_prices.get("charge_price", None))
                    
                if n_prices.get("discharge_price", None):
                    total_prices["discharge_price"].append(n_prices.get("discharge_price", None))

                c_kwh += nc_kwh
                c_pct += nc_pct
                c_cost += nc_cost
                c_local += nc_local
                c_grid += nc_grid

                d_kwh += nd_kwh
                d_pct += nd_pct
                d_cost += nd_cost
                savings_num += nsavings_num

                started = next_when  # oldest timestamp in the daily block
                j += 1

            idx = j
        else:
            idx += 1

        combined_block_counter += 1

        # --- Normalize and KEEP prices ---
        combined_db[started] = {
            "prices": {
                "buy_price": _round_or_none(sum(total_prices["buy_price"]) / len(total_prices["buy_price"]) if total_prices["buy_price"] else used_prices.get("buy_price", None), 3),
                "sell_price": _round_or_none(sum(total_prices["sell_price"]) / len(total_prices["sell_price"]) if total_prices["sell_price"] else used_prices.get("sell_price", None), 3),
                "powerwall_kwh_price": _round_or_none(sum(total_prices["powerwall_kwh_price"]) / len(total_prices["powerwall_kwh_price"]) if total_prices["powerwall_kwh_price"] else used_prices.get("powerwall_kwh_price", None), 3),
                "kwh_savings": _round_or_none(sum(total_prices["kwh_savings"]) / len(total_prices["kwh_savings"]) if total_prices["kwh_savings"] else used_prices.get("kwh_savings", None), 3),
                "charge_price": _round_or_none(sum(total_prices["charge_price"]) / len(total_prices["charge_price"]) if total_prices["charge_price"] else used_prices.get("charge_price", None), 3),
                "discharge_price": _round_or_none(sum(total_prices["discharge_price"]) / len(total_prices["discharge_price"]) if total_prices["discharge_price"] else used_prices.get("discharge_price", None), 3),
            },
            "charged": {
                "percentage": round(c_pct, 1),
                "kWh": round(c_kwh, 3),
                "kWh_from_grid": round(c_grid, 3),
                "kWh_from_local_energy": round(c_local, 3),
                "cost": round(c_cost, 3),
                "price": _round_or_none(c_cost / c_kwh if c_kwh else None, 3),
            },
            "discharged": {
                "percentage": round(d_pct, 1),
                "kWh": round(d_kwh, 3),
                "cost": round(d_cost, 3),
                "price": _round_or_none(d_cost / d_kwh if d_kwh else None, 3),
                "savings": _round_or_none(savings_num, 3),
            },
        }

        # Totals (based on combined block)
        month = getMonthFirstDay(started)
        _ensure_month(month)

        total["charged_kwh"][month] += c_kwh
        total["charged_cost"][month] += c_cost
        total["charged_local_kwh"][month] += c_local
        total["charged_grid_kwh"][month] += c_grid
        total["charged_pct"][month] += c_pct

        total["charged_kwh"]["total"] += c_kwh
        total["charged_cost"]["total"] += c_cost
        total["charged_local_kwh"]["total"] += c_local
        total["charged_grid_kwh"]["total"] += c_grid
        total["charged_pct"]["total"] += c_pct

        total["discharged_kwh"][month] += d_kwh
        total["discharged_cost"][month] += d_cost
        total["discharged_pct"][month] += d_pct

        total["discharged_kwh"]["total"] += d_kwh
        total["discharged_cost"]["total"] += d_cost
        total["discharged_pct"]["total"] += d_pct

        total["savings"][month] += savings_num
        total["savings"]["total"] += savings_num
        
        c_local_pct = c_pct * (c_local / c_kwh) if c_kwh > 0 else 0.0
        
        c_kwh = round(c_kwh, 1)
        c_local = round(c_local, 1)
        c_local_pct = round(c_local_pct, 0)
        c_pct = round(c_pct, 0)
        d_kwh = round(d_kwh, 1)
        d_pct = round(d_pct, 0)
        
        

        # --- folding / size ---
        if not history_too_big_announced:
            if current_history_bytes_size >= max_history_bytes_size:
                history_too_big_announced = True
                history_loop_append(f"##### {i18n.t('ui.charging_history_combine_and_set.too_big_warning')}\n")
            elif current_history_counter == length and not details:
                details = True
                history_loop_append([
                    "<details>",
                    f"<summary><b>{i18n.t('ui.charging_history_combine_and_set.more_history')}</b></summary>\n",
                ])
                add_header = True
            elif (
                current_history_bytes_size < max_history_bytes_size - 1
                and current_history_counter > length
                and current_history_counter >= sub_length
                and current_history_counter % sub_length == 0
                and (sorted_db_len - idx) >= sub_length
            ):
                history_loop_append([
                    "\n",
                    "<details>",
                    f"<summary><b>{i18n.t('ui.charging_history_combine_and_set.more_history')}</b></summary>\n",
                ])
                sub_details_count += 1
                add_header = True

        # --- UI rows (ONE ROW PER BLOCK/DAY) ---
        if current_history_bytes_size < max_history_bytes_size:
            if add_header:
                add_header = False
                history_loop_append([header, align])

            # If we're combining (daily), show date only; else show datetime
            if combined_block_counter > combine_after:
                time_str = f"**{started.strftime('%d/%m')}**"
            else:
                time_str = f"**{started.strftime('%d/%m %H:%M')}**"

            # % column: charge on first line (green), discharge on second line (red) if both exist
            pct_lines = []
            if c_kwh > 0.0 or c_pct > 0.0:
                pct_lines = [_charge_color(f"{int(c_pct)}")]
                if c_local_pct > 0.0:
                    pct_lines.append(f"{_solar_color(f'({int(c_local_pct)})')}")
            if d_kwh > 0.0 or d_pct > 0.0:
                pct_lines.append(_discharge_color(f"-{int(d_pct)}"))
            pct_str = "<br>".join(pct_lines) if pct_lines else ""

            # kWh column: same pattern; include solar local split on charge line if you want
            kwh_lines = []
            if c_kwh > 0.0:
                c_line = _charge_color(f"{_fmt(c_kwh,1,'0.0')}")
                if c_local == c_kwh:
                    c_line = f"{_solar_color(_fmt(c_local,1,'0.0'))}"
                elif c_local > 0.0:
                    c_line += f"<br>({_solar_color(_fmt(c_local,1,'0.0'))})"
                kwh_lines.append(c_line)
            if d_kwh > 0.0:
                kwh_lines.append(_discharge_color(f"-{_fmt(d_kwh,1,'0.0')}"))
            kwh_str = "<br>".join(kwh_lines) if kwh_lines else ""

            # price columns
            buy_price = used_prices.get("buy_price", None)
            charge_price_unit = (c_cost / c_kwh) if c_kwh > 0 else None
            charge_price_lines = []
            if combined_block_counter < combine_after and d_kwh > 0.0 and buy_price is not None:
                buy_price_str = f"🔌{_fmt(buy_price * d_kwh,2,'0.00')}<br>({_fmt(buy_price,2,'')})"
                charge_price_lines.append(_buy_color(buy_price_str))
            if c_kwh > 0.0:
                charge_price_lines.append(f"{_fmt(c_cost,2,'0.00')}<br>({_fmt(charge_price_unit,2,'')})")
            charge_price_str = "<br>".join(charge_price_lines) if charge_price_lines else ""

            discharge_price_unit = (d_cost / d_kwh) if d_kwh > 0 else None
            discharge_price_str = ""
            if d_kwh > 0.0:
                discharge_price_str = f"{_fmt(d_cost,2,'0.00')}<br>({_fmt(discharge_price_unit,2,'')})"

            sav_lines = []
            if savings_num != 0.0:
                sav_lines.append(f"{_fmt(savings_num,2,'0.00')}")
                if combined_block_counter < combine_after and d_kwh > 0.0 and buy_price is not None:
                    sav_lines.append(f"({_fmt(buy_price - discharge_price_unit,2,'0.00')})")
            sav_str = "<br>".join(sav_lines) if round(savings_num, 2) != 0.0 else ""

            if pct_str or kwh_str or charge_price_str or discharge_price_str:
                history_loop_append(f"| {time_str} | {pct_str} | {kwh_str} | {_charge_color(charge_price_str)} | {_discharge_color(discharge_price_str)} | {sav_str} |")
            current_history_counter += 1

        # close details at end
        if current_history_counter > length and (sorted_db_len - idx) == 0:
            history_loop_append(["</details>", "\n"] * (sub_details_count + 1))

    if details:
        history.extend(["</details>", "\n"])

    # --- Totals summary ---
    ck_tot = total["charged_kwh"]["total"]
    dk_tot = total["discharged_kwh"]["total"]
    cc_tot = total["charged_cost"]["total"]
    dc_tot = total["discharged_cost"]["total"]
    sav_tot = total["savings"]["total"]

    total_charge_price_unit = (cc_tot / ck_tot) if ck_tot > 0 else None
    total_charge_price_str = f"<br>({_fmt(total_charge_price_unit,2,'')})" if total_charge_price_unit is not None else ""

    total_discharge_price_unit = (dc_tot / dk_tot) if dk_tot > 0 else None
    total_discharge_price_str = f"<br>({_fmt(total_discharge_price_unit,2,'')})" if total_discharge_price_unit is not None else ""

    total_savings_price_unit = (sav_tot / dk_tot) if dk_tot > 0 else None
    total_savings_price_str = f"<br>({_fmt(total_savings_price_unit,2,'')})" if total_savings_price_unit is not None else ""

    net_kwh = ck_tot - dk_tot
    net_cost = cc_tot - dc_tot
    
    spacing_style = "display:inline-block; text-align:center;"

    col_charge = (
        f"<span style='{spacing_style}'>"
        f"{emoji_parse({'charging': True})}{_fmt(ck_tot,1,'0.0')}kWh&nbsp;&nbsp;&nbsp;"
        f"{cc_tot:.2f}{i18n.t('ui.common.valuta')}<br>"
        f"</span>"
    )

    col_discharge = (
        f"<span style='{spacing_style}'>"
        f"{emoji_parse({'discharging': True})}{_fmt(dk_tot,1,'0.0')}kWh&nbsp;&nbsp;&nbsp;"
        f"{dc_tot:.2f}{i18n.t('ui.common.valuta')}<br>"
        f"</span>"
    )

    col_net = (
        f"<span style='{spacing_style}'>"
        f"{emoji_parse({'balance': True})}{_fmt(net_kwh,1,'0.0')}kWh&nbsp;&nbsp;&nbsp;"
        f"{net_cost:.2f}{i18n.t('ui.common.valuta')}<br>"
        f"</span>"
    )

    first_line = f"{i18n.t('ui.common.total')}: {_charge_color(col_charge)}{_discharge_color(col_discharge)}{col_net}"

    history.append("---")
    history.append("<details>")
    history.append(
        f"\n<summary><b>"
        f"{first_line}"
        f"{emoji_parse({'profit': True})} {sav_tot:.2f}{i18n.t('ui.common.valuta')}"
        f"</b></summary>\n"
    )

    history.extend([
        f"| {i18n.t('ui.charging_history_combine_and_set.month')} | kWh<br>{emoji_parse({'charging': True})} | kWh<br>{emoji_parse({'discharging': True})} | kWh<br>{emoji_parse({'balance': True})} | {i18n.t('ui.common.price')}<br>{emoji_parse({'charging': True})} | {i18n.t('ui.common.price')}<br>{emoji_parse({'discharging': True})} | {emoji_parse({'profit': True})} |",
        "|:---:|:---:|:---:|:---:|:---:|:---:|:---:|",
    ])

    month_keys = [k for k in total["charged_kwh"].keys() if isinstance(k, datetime.datetime)]
    for idx_m, month in enumerate(sorted(month_keys)):
        ck = total["charged_kwh"][month]
        dk = total["discharged_kwh"][month]
        nc = ck - dk

        cc = total["charged_cost"][month]
        dc = total["discharged_cost"][month]
        sav = total["savings"][month]

        charge_price_unit = (cc / ck) if ck > 0 else None
        charge_price_str = "" if charge_price_unit is None else f"<br>({_fmt(charge_price_unit,2,'')})"

        discharge_price_unit = (dc / dk) if dk > 0 else None
        discharge_price_str = "" if discharge_price_unit is None else f"<br>({_fmt(discharge_price_unit,2,'')})"

        savings_price_unit = (sav / dk) if dk > 0 else None
        savings_price_str = "" if savings_price_unit is None else f"<br>({_fmt(savings_price_unit,2,'')})"

        bg_s = "<font color=grey>" if idx_m % 2 == 0 else ""
        bg_e = "</font>" if idx_m % 2 == 0 else ""

        month_string = month.strftime("%B").lower()
        month_name = i18n.t(f"ui.calendar.month_names.{month_string}")

        history.append(
            f"| {bg_s}{month_name}<br>{month.strftime('%Y')}{bg_e} | "
            f"{bg_s}{ck:.1f}{bg_e} | {bg_s}-{dk:.1f}{bg_e} | {bg_s}{nc:.1f}{bg_e} | "
            f"{bg_s}{cc:.2f}{charge_price_str}{bg_e} | {bg_s}{dc:.2f}{discharge_price_str}{bg_e} | {bg_s}{sav:.2f}{savings_price_str}{bg_e} |"
        )

    history.append(
        f"| **{i18n.t('ui.common.total')}** | **{ck_tot:.1f}** | **-{dk_tot:.1f}** | **{net_kwh:.1f}** | "
        f"**{cc_tot:.2f}{total_charge_price_str}** | **{dc_tot:.2f}{total_discharge_price_str}** | **{sav_tot:.2f}{total_savings_price_str}** |"
    )

    history.append("\n</details>\n")
    history.append("</center>")

    # --- Ending-byte-size buffer ---
    total_history_bytes_size = len("\n".join(history).encode("utf-8"))
    CHARGING_HISTORY_ENDING_BYTE_SIZE = total_history_bytes_size - current_history_bytes_size + 1000

    if not get_ending_byte_size:
        set_attr(
            f"sensor.{__name__}_charging_history.history",
            "\n".join(history) if history else "**Ingen lade historik**",
        )
        CHARGING_HISTORY_DB = combined_db

@benchmark_decorator()
@service(f"pyscript.{__name__}_recalc_charging_history_today")
def recalc_charging_history_today():
    """yaml
    name: "BattMind: Recalc charging history today"
    description: Recalculate charging history for today (hourly blocks)
    """
    func_name = "recalc_charging_history_today"
    func_prefix = f"{func_name}_"
    _LOGGER = globals()["_LOGGER"].getChild(func_name)
    global TASKS, CHARGING_HISTORY_DB

    _LOGGER.info("Starting recalculation of today's charging history (hourly blocks)")
    
    now_hour = getHour()
    
    set_charging_rule(f"📟Recalculating today's charging history 0:00-{max(now_hour-1, 0)}:59")
    my_persistent_notification(
        f"📌 Starting {TITLE} recalculation of today's charging history (hourly blocks).\nThis may take a few minutes. Please wait...",
        title=f"{TITLE} recalculation",
        persistent_notification_id=f"{__name__}_{func_name}"
    )
    task.wait_until(timeout=0.5)
    
    task_set = set()
    timestamp = reset_time_to_hour()

    for ts in list(CHARGING_HISTORY_DB.keys()):
        if isinstance(ts, datetime.datetime) and ts.date() == timestamp.date():
            _LOGGER.info(f"Removing existing charging history entry for {ts} to prepare for recalculation")
            CHARGING_HISTORY_DB.pop(ts, None)
    
    for hour in range(0, now_hour):
        task_name = f"{func_prefix}charging_history_{hour:02d}"
        ts = timestamp.replace(hour=hour, minute=0, second=0, microsecond=0)

        TASKS[task_name] = task.create(
            charging_history,
            timestamp=ts,
            save_db=False
        )
        TASKS[task_name].set_name(task_name)
        task_set.add(TASKS[task_name])

    done, pending = task.wait(task_set)
    set_charging_rule(f"📟Recalculating today's last charging history {now_hour}:00-{now_hour}:59...")
    task_name = f"{func_prefix}charging_history_{now_hour}"
    TASKS[task_name] = task.create(
            charging_history,
            timestamp=timestamp.replace(hour=now_hour, minute=0, second=0, microsecond=0),
            save_db=True
        )
    TASKS[task_name].set_name(task_name)
    task_set.add(TASKS[task_name])
    
    done, pending = task.wait(task_set)

    failures = 0
    for t in done:
        try:
            result = t.result()  # <-- her kommer fejl frem
            _LOGGER.info(f"Charging history task {t.get_name()} completed successfully with result: {result}")
        except Exception as e:
            failures += 1
            _LOGGER.error(f"Charging history task failed: {e} ({type(e)})")

    if pending:
        _LOGGER.warning(f"{len(pending)} tasks still pending after wait()")

    if failures:
        _LOGGER.warning(f"Recalc finished with {failures} failures (DB save may be incomplete)")
        my_persistent_notification(
            f"⚠️ Finished {TITLE} recalculation of today's charging history (hourly blocks) with {failures} failures. Check logs for details.",
            title=f"{TITLE} recalculation complete with errors",
            persistent_notification_id=f"{__name__}_{func_name}"
        )
    else:
        _LOGGER.info("Recalc finished successfully")
        
        set_charging_rule(f"📟Finished recalculation of today's charging history")
        my_persistent_notification(
            f"✅ Finished {TITLE} recalculation of today's charging history (hourly blocks) without errors.",
            title=f"{TITLE} recalculation complete",
            persistent_notification_id=f"{__name__}_{func_name}"
        )

@retry(times=3, exceptions=(Exception,), delay=2, backoff=2)
async def charging_history(timestamp=None, save_db = True):
    func_name = "charging_history"
    func_prefix = f"{func_name}_"
    func_id = random.randint(100000, 999999)
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    global CHARGING_HISTORY_DB, TASKS, BATTERY_LEVEL_EXPENSES
    
    if not BATTERY_LEVEL_EXPENSES or save_db:
        current_battery_level_expenses()
    
    try:
        if len(CHARGING_HISTORY_DB) == 0:
            load_charging_history()
        
        start = reset_time_to_hour()
        
        if timestamp is not None:
            start = reset_time_to_hour(timestamp)
            
        end = start.replace(minute=59, second=59, microsecond=0)
        _LOGGER.info(f"Calculating charging history for period: {start.strftime('%Y-%m-%d %H:%M:%S')}-{end.strftime('%H:%M:%S')}")
        
        data = power_values(start, end)
        #_LOGGER.info(f"data:{pformat(data)}")
        
        home_consumption_kwh = data['power_consumption'] / 1000.0
        charge_kwh = data['powerwall_charging_consumption'] / 1000.0
        discharge_kwh = data['powerwall_discharging_consumption'] / 1000.0
        solar_production_kwh = data['solar_production'] / 1000.0
        
        home_consumption_without_solar_kwh = max(home_consumption_kwh - solar_production_kwh, 0.0)
        grid_consumption_kwh = home_consumption_kwh - (discharge_kwh + solar_production_kwh)
        
        charge_grid_share_pct = max(grid_consumption_kwh / charge_kwh if charge_kwh > 0.0 else 0.0, 0.0)
        discharge_grid_share_pct = max(grid_consumption_kwh / discharge_kwh if discharge_kwh > 0.0 else 0.0, 0.0)
        
        solar_charge_share_pct = max(solar_production_kwh / charge_kwh if charge_kwh > 0.0 else 0.0, 0.0)
        solar_discharge_share_pct = max(solar_production_kwh / (discharge_kwh + home_consumption_kwh) if discharge_kwh > 0.0 else 0.0, 0.0)
        
        powerwall_discharge_share_pct = max(discharge_kwh / (home_consumption_kwh - solar_production_kwh) if (home_consumption_kwh - solar_production_kwh) > 0.0 else 0.0, 0.0)
        
        share_pct_sum = charge_grid_share_pct + solar_charge_share_pct + discharge_grid_share_pct + solar_discharge_share_pct + powerwall_discharge_share_pct
        normalization_factor = share_pct_sum / 1.0 if share_pct_sum > 0.0 else 1.0
        
        charge_grid_share_pct /= normalization_factor
        solar_charge_share_pct /= normalization_factor
        discharge_grid_share_pct /= normalization_factor
        solar_discharge_share_pct /= normalization_factor
        powerwall_discharge_share_pct /= normalization_factor
        
        _LOGGER.info(f"debug {start} home_consumption_kwh: {home_consumption_kwh:.3f} kWh, charge_kwh: {charge_kwh:.3f} kWh, discharge_kwh: {discharge_kwh:.3f} kWh, solar_production_kwh: {solar_production_kwh:.3f} kWh, grid_consumption_kwh: {grid_consumption_kwh:.3f} kWh")
        _LOGGER.info(f"debug {start} charge_grid_share_pct: {charge_grid_share_pct * 100:.1f}%, discharge_grid_share_pct: {discharge_grid_share_pct * 100:.1f}%, powerwall_discharge_share_pct: {powerwall_discharge_share_pct * 100:.1f}%")
        _LOGGER.info(f"debug {start} solar_charge_share_pct: {solar_charge_share_pct * 100:.1f}%, solar_discharge_share_pct: {solar_discharge_share_pct * 100:.1f}%")
        
        """selling_to_grid = True if grid_consumption_kwh <= -100.0 else False
        selling_to_grid = True if solar_production_kwh >= home_consumption_kwh else selling_to_grid"""
        
        buy_price = get_hour_prices().get(start, None)
        sell_price = get_hour_prices(sell_prices = True).get(start, None)
        
        powerwall_kwh_price = BATTERY_LEVEL_EXPENSES.get("battery_level_expenses_unit", None) if charge_kwh == 0.0 else None
        
        if buy_price is None or sell_price is None:
            raise Exception(f"Buy or sell price not found for timestamp {start}. buy_price: {buy_price}, sell_price: {sell_price}")
        
        if not isinstance(powerwall_kwh_price, (int, float)):
            powerwall_kwh_price = get_powerwall_kwh_price()
        
        charge_grid_share = buy_price * charge_grid_share_pct
        discharge_grid_share = buy_price * discharge_grid_share_pct
        
        solar_charge_share = sell_price * solar_charge_share_pct
        solar_discharge_share = sell_price * solar_discharge_share_pct
        
        powerwall_discharge_share = powerwall_kwh_price * powerwall_discharge_share_pct
        
        charge_price = sum([charge_grid_share, solar_charge_share])
        discharge_price = sum([discharge_grid_share, solar_discharge_share, powerwall_discharge_share])
        
        _LOGGER.warning(f"debug {start} buy_price: {buy_price:.3f} kr/kWh, sell_price: {sell_price:.3f} kr/kWh, powerwall_kwh_price: {powerwall_kwh_price:.3f} kr/kWh")
        _LOGGER.warning(f"debug {start} charge_grid_share: {charge_grid_share:.3f} kr/kWh, solar_charge_share: {solar_charge_share:.3f} kr/kWh, discharge_grid_share: {discharge_grid_share:.3f} kr/kWh, solar_discharge_share: {solar_discharge_share:.3f} kr/kWh, powerwall_discharge_share: {powerwall_discharge_share:.3f} kr/kWh")
        _LOGGER.warning(f"debug {start} charge_price: {charge_price:.3f} kr/kWh, discharge_price: {discharge_price:.3f} kr/kWh")
        
        #kwh_savings = round(buy_price - powerwall_kwh_price, 3) if isinstance(buy_price, (int, float)) and isinstance(powerwall_kwh_price, (int, float)) else None
        
        charge_cost = charge_price * charge_kwh if isinstance(charge_price, (int, float)) else None
        discharge_cost = discharge_price * discharge_kwh if isinstance(discharge_price, (int, float)) else None
        #savings = discharge_kwh * kwh_savings
        savings = (discharge_kwh * buy_price) - (discharge_kwh * discharge_price) if isinstance(buy_price, (int, float)) and isinstance(discharge_price, (int, float)) else None
        kwh_savings = savings / discharge_kwh if isinstance(savings, (int, float)) and discharge_kwh != 0 else None
        
        """if selling_to_grid:
            if isinstance(sell_price, (int, float)):
                _LOGGER.info(f"{start} Selling to grid, using sell price for charge cost calculation: {sell_price} kr/kWh grid_consumption_kwh: {grid_consumption_kwh} kWh")
                charge_price = sell_price
                charge_cost = charge_kwh * sell_price
                
                _LOGGER.info(f"{start} Selling to grid, using sell price for discharge cost calculation: {sell_price} kr/kWh grid_consumption_kwh: {grid_consumption_kwh} kWh")
                discharge_price = sell_price
                discharge_cost = discharge_kwh * sell_price
                
        else:
            if isinstance(buy_price, (int, float)):
                #_LOGGER.info(f"{start} Not selling to grid, using buy price for charge cost calculation: {buy_price} kr/kWh grid_consumption_kwh: {grid_consumption_kwh} kWh")
                charge_price = buy_price
                charge_cost = charge_kwh * buy_price
                
            #_LOGGER.info(f"{start} Not selling to grid, using powerwall kWh price for discharge cost calculation: {powerwall_kwh_price} kr/kWh grid_consumption_kwh: {grid_consumption_kwh} kWh")
            discharge_price = powerwall_kwh_price
            discharge_cost = discharge_kwh * powerwall_kwh_price"""
            
        kwh_from_local_energy = 0.0
        
        if charge_kwh > 0.0:
            try:
                TASKS[f"{func_prefix}calc_local_energy_kwh_{func_id}"] = task.create(calc_local_energy_kwh, start, end, charge_kwh)
                done, pending = task.wait({TASKS[f"{func_prefix}calc_local_energy_kwh_{func_id}"]})
                
                _, kwh_from_local_energy = TASKS[f"{func_prefix}calc_local_energy_kwh_{func_id}"].result()
                kwh_from_local_energy = min(kwh_from_local_energy, charge_kwh)
            except (asyncio.CancelledError, asyncio.TimeoutError, KeyError) as e:
                _LOGGER.error(f"Task for calculating local energy kWh or price was cancelled or timed out: {e} {type(e)}")
                my_persistent_notification(
                    f"Charging history task for calculating local energy kWh or price was cancelled or timed out: {e} {type(e)}",
                    f"{TITLE} failed",
                    persistent_notification_id=f"{__name__}_{func_name}_task_cancelled_or_timed_out"
                )
            except Exception as e:
                _LOGGER.error(f"Error calculating local energy kWh or price: {e} {type(e)}")
                my_persistent_notification(
                    f"Charging history failed to calculate local energy kWh or price: {e} {type(e)}",
                    f"{TITLE} failed",
                    persistent_notification_id=f"{__name__}_{func_name}_calc_local_energy_kwh_or_price_failed"
                )
            finally:
                task_cancel(func_prefix, task_remove=True, startswith=True)
        
        session_dict = {
            "prices": {
                "buy_price": round(buy_price, 3) if isinstance(buy_price, (int, float)) else None,
                "sell_price": round(sell_price, 3) if isinstance(sell_price, (int, float)) else None,
                "powerwall_kwh_price": round(powerwall_kwh_price, 3) if isinstance(powerwall_kwh_price, (int, float)) else None,
                "kwh_savings": round(kwh_savings, 3) if isinstance(kwh_savings, (int, float)) else None,
                "charge_price": round(charge_price, 3) if isinstance(charge_price, (int, float)) else None,
                "discharge_price": round(discharge_price, 3) if isinstance(discharge_price, (int, float)) else None,
            },
            "charged": {
                "percentage": round(kwh_to_percentage(charge_kwh), 1),
                "kWh": round(charge_kwh, 3),
                "kWh_from_grid": round(max(charge_kwh - kwh_from_local_energy, 0.0), 3),
                "kWh_from_local_energy": round(kwh_from_local_energy, 3),
                "cost": round(charge_cost, 3) if isinstance(charge_cost, (int, float)) else None,
                "price": round(charge_price, 3) if isinstance(charge_price, (int, float)) else None
            },
            "discharged": {
                "percentage": round(kwh_to_percentage(discharge_kwh), 1),
                "kWh": round(discharge_kwh, 3),
                "cost": round(discharge_cost, 3) if isinstance(discharge_cost, (int, float)) else None,
                "price": round(discharge_price, 3) if isinstance(discharge_price, (int, float)) else None,
                "savings": round(savings, 3) if isinstance(savings, (int, float)) else None
            }
        }
        
        if charge_kwh > 0.0 or discharge_kwh > 0.0:
            _LOGGER.info(f"Adding charging history entry for {start}: charge_kwh: {charge_kwh:.3f} kWh, discharge_kwh: {discharge_kwh:.3f} kWh, charge_cost: {charge_cost:.2f} kr, discharge_cost: {discharge_cost:.2f} kr")
            CHARGING_HISTORY_DB[start] = session_dict
        
        if not save_db:
            return session_dict
            
        try:
            charging_history_combine_and_set()
            save_charging_history()
        except Exception as e:
            _LOGGER.error(f"Error combining and setting charging history after adding new session: {e} {type(e)}")
            my_persistent_notification(
                f"Charging history failed to combine and set after adding new session: {e} {type(e)}",
                f"{TITLE} failed",
                persistent_notification_id=f"{__name__}_{func_name}_combine_and_set_failed"
            )
            
        
        return session_dict
    except Exception as e:
        error_message = f"Error in {func_name} error: {e} {type(e)}"
        _LOGGER.error(error_message)
        
        save_error_to_file(error_message, caller_function_name = f"{func_name}()")
        my_persistent_notification(
            f"Charging history failed with\nerror: {e} {type(e)}",
            f"{TITLE} error",
            persistent_notification_id=f"{__name__}_{func_name}_failed"
        )
        
        raise

def current_battery_level_expenses():
    func_name = "current_battery_level_expenses"
    func_prefix = f"{func_name}_"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    global BATTERY_LEVEL_EXPENSES
    
    BATTERY_LEVEL_EXPENSES = {
        "battery_level_expenses_kwh": 0.0,
        "battery_level_expenses_percentage": 0.0,
        "battery_level_expenses_solar_percentage": 0.0,
        "battery_level_expenses_cost": 0.0,
        "battery_level_expenses_unit": None,
        "battery_level_expenses_cost_loss": 0.0,
        "battery_level_expenses_cost_wear": 0.0,
        "battery_level_expenses_cost_with_loss_and_wear": 0.0,
        "battery_level_expenses_unit_with_loss_and_wear": None,
    }
    
    try:
        current_battery_level = get_battery_level()# - max(get_min_daily_battery_level(), get_min_trip_battery_level())
        
        if CHARGING_HISTORY_DB:
            for key in dict(sorted(CHARGING_HISTORY_DB.items(), key=lambda item: item[0], reverse=True)).keys():
                if round(BATTERY_LEVEL_EXPENSES["battery_level_expenses_percentage"]) < round(current_battery_level):
                    if (
                        "price" not in CHARGING_HISTORY_DB[key]['charged'] or
                        "cost" not in CHARGING_HISTORY_DB[key]['charged'] or
                        "kWh" not in CHARGING_HISTORY_DB[key]['charged'] or
                        "kWh_from_local_energy" not in CHARGING_HISTORY_DB[key]['charged'] or
                        "percentage" not in CHARGING_HISTORY_DB[key]['charged']
                    ):
                        continue
                    
                    if not isinstance(CHARGING_HISTORY_DB[key]['charged']["price"], (int, float)):
                        continue
                    
                    price = CHARGING_HISTORY_DB[key]['charged']["price"]
                    cost = CHARGING_HISTORY_DB[key]['charged']["cost"]
                    kwh = CHARGING_HISTORY_DB[key]['charged']["kWh"]
                    percentage = CHARGING_HISTORY_DB[key]['charged']["percentage"]
                    solar_percentage = kwh_to_percentage(CHARGING_HISTORY_DB[key]['charged']["kWh_from_local_energy"], include_charging_loss=True)
                    cost_loss = calc_battery_loss_cost(cost)
                    total_cost = (cost + cost_loss) + abs(CONFIG['solar']['powerwall_wear_cost_per_kwh'])
                    
                    new_battery_level = percentage + BATTERY_LEVEL_EXPENSES["battery_level_expenses_percentage"]
                    
                    if new_battery_level > current_battery_level and percentage > 0.0:
                        diff = (percentage - (new_battery_level - current_battery_level)) / percentage
                        cost *= diff
                        cost_loss *= diff
                        total_cost *= diff
                        kwh *= diff
                        percentage *= diff
                        solar_percentage *= diff
                        
                    BATTERY_LEVEL_EXPENSES[key] = CHARGING_HISTORY_DB[key]['charged']
                    
                    BATTERY_LEVEL_EXPENSES["battery_level_expenses_kwh"] += kwh
                    BATTERY_LEVEL_EXPENSES["battery_level_expenses_percentage"] += percentage
                    BATTERY_LEVEL_EXPENSES["battery_level_expenses_solar_percentage"] += solar_percentage
                    BATTERY_LEVEL_EXPENSES["battery_level_expenses_cost"] += cost
                    BATTERY_LEVEL_EXPENSES["battery_level_expenses_cost_loss"] += cost_loss
                    BATTERY_LEVEL_EXPENSES["battery_level_expenses_cost_wear"] += abs(CONFIG['solar']['powerwall_wear_cost_per_kwh']) * kwh
                    BATTERY_LEVEL_EXPENSES["battery_level_expenses_cost_with_loss_and_wear"] += total_cost
                else:
                    break
                
            BATTERY_LEVEL_EXPENSES["battery_level_expenses_kwh"] = round(BATTERY_LEVEL_EXPENSES["battery_level_expenses_kwh"], 3)
            BATTERY_LEVEL_EXPENSES["battery_level_expenses_percentage"] = round(BATTERY_LEVEL_EXPENSES["battery_level_expenses_percentage"], 1)
            BATTERY_LEVEL_EXPENSES["battery_level_expenses_solar_percentage"] = round(BATTERY_LEVEL_EXPENSES["battery_level_expenses_solar_percentage"], 1)
            BATTERY_LEVEL_EXPENSES["battery_level_expenses_cost"] = round(BATTERY_LEVEL_EXPENSES["battery_level_expenses_cost"], 3)
            BATTERY_LEVEL_EXPENSES["battery_level_expenses_cost_with_loss_and_wear"] = round(BATTERY_LEVEL_EXPENSES["battery_level_expenses_cost_with_loss_and_wear"], 3)
            
            if BATTERY_LEVEL_EXPENSES["battery_level_expenses_kwh"] > 0.0:
                BATTERY_LEVEL_EXPENSES['battery_level_expenses_unit'] = round(BATTERY_LEVEL_EXPENSES["battery_level_expenses_cost"] / BATTERY_LEVEL_EXPENSES["battery_level_expenses_kwh"], 3)
                BATTERY_LEVEL_EXPENSES['battery_level_expenses_unit_with_loss_and_wear'] = round(BATTERY_LEVEL_EXPENSES["battery_level_expenses_cost_with_loss_and_wear"] / BATTERY_LEVEL_EXPENSES["battery_level_expenses_kwh"], 3)

    except Exception as e:
        _LOGGER.warning(f"Error in battery level cost calculation: {e} {type(e)}")

    return BATTERY_LEVEL_EXPENSES

def calc_battery_loss_cost(price):
    return price * abs(CONFIG['solar']['powerwall_charge_discharge_loss'])

def update_grid_prices():
    func_name = "update_grid_prices"
    func_prefix = f"{func_name}_"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    global TASKS
        
    try:
        TASKS[f"{func_prefix}"] = task.create(get_hour_prices, update_prices = True)
        done, pending = task.wait({TASKS[f"{func_prefix}"]})
    except (asyncio.CancelledError, asyncio.TimeoutError, KeyError) as e:
        _LOGGER.warning(f"Task for updating grid prices was cancelled or timed out: {e} {type(e)}")
        return
    except Exception as e:
        _LOGGER.error(f"Error in {func_name}: {e} {type(e)}")
        my_persistent_notification(
            f"Error in {func_name}: {e} {type(e)}",
            title=f"{TITLE} error",
            persistent_notification_id=f"{__name__}_{func_name}_error"
        )
    finally:
        task_cancel(func_prefix, task_remove=True, startswith=True)

def get_hour_prices(update_prices = False, sell_prices = False):
    #TODO See development in hourly prices variation, if 15 min interval is better, than 1 hour average
    func_name = "get_hour_prices"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    global TASKS, LAST_SUCCESSFUL_GRID_PRICES
    
    now = getTime()
    current_hour = reset_time_to_hour(now)
    
    hour_prices = {}
    price_adder_day_between_divider = 30
    
    try:
        all_prices_loaded = True
        
        if CONFIG['prices']['entity_ids']['power_prices_entity_id'] not in state.names(domain="sensor"):
            raise Exception(f"{CONFIG['prices']['entity_ids']['power_prices_entity_id']} not loaded")
        
        power_prices_attr = get_attr(CONFIG['prices']['entity_ids']['power_prices_entity_id'], error_state={})
            
        if ("prices" in LAST_SUCCESSFUL_GRID_PRICES and
            LAST_SUCCESSFUL_GRID_PRICES['using_offline_prices'] is False and
            update_prices is False):
            if "update_grid_prices" in TASKS and not TASKS["update_grid_prices"].done():
                _LOGGER.warning("Waiting for update_grid_prices to complete")
                task_wait_until("update_grid_prices", timeout=120)
            
            hour_prices = deepcopy(LAST_SUCCESSFUL_GRID_PRICES["prices"])
        else:
            if "raw_today" in power_prices_attr:
                for raw in power_prices_attr['raw_today']:
                    hour_string = "hour" if "hour" in raw else "time"
                    
                    raw[hour_string] = toDateTime(raw[hour_string])
                    
                    if (isinstance(raw[hour_string], datetime.datetime) and
                        isinstance(raw['price'], (int, float)) and
                        daysBetween(current_hour, raw[hour_string]) == 0):
                        hour = reset_time_to_hour(raw[hour_string])
                        
                        if hour not in hour_prices:
                            hour_prices[hour] = []
                            
                        hour_prices[hour].append(raw['price'])
                    else:
                        all_prices_loaded = False
                        
            if "forecast" in power_prices_attr:
                for raw in power_prices_attr['forecast']:
                    hour_string = "hour" if "hour" in raw else "time"
                    
                    raw[hour_string] = toDateTime(raw[hour_string])
                    
                    if (isinstance(raw[hour_string], datetime.datetime) and
                        isinstance(raw['price'], (int, float)) and
                        daysBetween(current_hour, raw[hour_string]) > 0):
                        hour = reset_time_to_hour(raw[hour_string])
                        
                        if hour not in hour_prices:
                            hour_prices[hour] = []
                            
                        hour_prices[hour].append(raw['price'] + (daysBetween(current_hour, hour) / price_adder_day_between_divider))
                    else:
                        all_prices_loaded = False

            if "tomorrow_valid" in power_prices_attr:
                if power_prices_attr['tomorrow_valid']:
                    if "raw_tomorrow" not in power_prices_attr or len(power_prices_attr['raw_tomorrow']) < 23: #Summer and winter time compensation
                        _LOGGER.warning(f"Raw_tomorrow not in {CONFIG['prices']['entity_ids']['power_prices_entity_id']} attributes, raw_tomorrow len({len(power_prices_attr['raw_tomorrow'])})")
                    else:
                        for raw in power_prices_attr['raw_tomorrow']:
                            hour_string = "hour" if "hour" in raw else "time"
                    
                            raw[hour_string] = toDateTime(raw[hour_string])
                            
                            if (isinstance(raw[hour_string], datetime.datetime) and
                                isinstance(raw['price'], (int, float)) and
                                daysBetween(current_hour, raw[hour_string]) == 1):
                                hour = reset_time_to_hour(raw[hour_string])
                                
                                if hour not in hour_prices:
                                    hour_prices[hour] = []
                                    
                                hour_prices[hour].append(raw['price'])
                            else:
                                all_prices_loaded = False
                                    
            for hour in hour_prices:
                if isinstance(hour_prices[hour], list):
                    hour_prices[hour] = round(average(hour_prices[hour]) - get_refund(), 2)
            
            if "raw_today" not in power_prices_attr:
                raise Exception(f"Real prices not in {CONFIG['prices']['entity_ids']['power_prices_entity_id']} attributes")
            elif len(power_prices_attr['raw_today']) < 23: #Summer and winter time compensation
                raise Exception(f"Not all real prices in {CONFIG['prices']['entity_ids']['power_prices_entity_id']} attributes, raw_today len({len(power_prices_attr['raw_today'])})")

            if "forecast" not in power_prices_attr:
                raise Exception(f"Forecast not in {CONFIG['prices']['entity_ids']['power_prices_entity_id']} attributes")
            elif len(power_prices_attr['forecast']) < 100: #Full forecast length is 142
                raise Exception(f"Not all forecast prices in {CONFIG['prices']['entity_ids']['power_prices_entity_id']} attributes, forecast len({len(power_prices_attr['forecast'])})")

            if not all_prices_loaded:
                raise Exception(f"Not all prices loaded in {CONFIG['prices']['entity_ids']['power_prices_entity_id']} attributes")
            else:
                LAST_SUCCESSFUL_GRID_PRICES.pop("missing_hours", None)
                
                LAST_SUCCESSFUL_GRID_PRICES["last_update"] = getTime()
                LAST_SUCCESSFUL_GRID_PRICES["prices"] = hour_prices
                LAST_SUCCESSFUL_GRID_PRICES['using_offline_prices'] = False
    except Exception as e:
        if "last_update" in LAST_SUCCESSFUL_GRID_PRICES and minutesBetween(LAST_SUCCESSFUL_GRID_PRICES["last_update"], now) <= 120:
            hour_prices = deepcopy(LAST_SUCCESSFUL_GRID_PRICES["prices"])
            _LOGGER.warning(f"Not all prices loaded in {CONFIG['prices']['entity_ids']['power_prices_entity_id']} attributes, using last successful")
        else:
            _LOGGER.warning(f"Cant get all online prices, using database: {e} {type(e)}")

            LAST_SUCCESSFUL_GRID_PRICES['using_offline_prices'] = True
            missing_hours = {}
            try:
                if "history" not in KWH_AVG_PRICES_DB:
                    raise Exception(f"Missing history in KWH_AVG_PRICES_DB")
                
                for h in range(24):
                    for d in range(7):
                        if d not in KWH_AVG_PRICES_DB['history'][h]:
                            raise Exception(f"Missing hour {h} and day of week {d} in KWH_AVG_PRICES_DB")

                        timestamp = reset_time_to_hour(current_hour.replace(hour=h)) + datetime.timedelta(days=d)
                        timestamp = timestamp.replace(tzinfo=None)
                        
                        if timestamp in hour_prices:
                            continue
                        
                        avg_price = average(KWH_AVG_PRICES_DB['history'][h][d]) # Refund is already included in KWH_AVG_PRICES_DB
                        price = round(avg_price + (daysBetween(current_hour, timestamp) / price_adder_day_between_divider), 2)
                        
                        missing_hours[timestamp] = price
                        hour_prices[timestamp] = price
                        
                if missing_hours:
                    missing_hours = dict(sorted(missing_hours.items()))
                    _LOGGER.info(f"Using following offline prices: {missing_hours}")
                    
                    LAST_SUCCESSFUL_GRID_PRICES["missing_hours"] = missing_hours
                    
            except Exception as e:
                error_message = f"Cant get offline prices: {e} {type(e)}"
                _LOGGER.error(error_message)
                save_error_to_file(error_message, caller_function_name = f"{func_name}()")
                my_persistent_notification(f"Kan ikke hente offline priser: {e} {type(e)}", f"{TITLE} error", persistent_notification_id=f"{__name__}_{func_name}_offline_prices_error")
                raise Exception(f"Offline prices error: {e} {type(e)}")
    
    if sell_prices:
        for timestamp, price in deepcopy(hour_prices).items():
            
            day_of_week = getDayOfWeek(timestamp)
            tariff_dict = get_tariffs(getHour(), day_of_week)
            transmissions_nettarif = tariff_dict["transmissions_nettarif"]
            systemtarif = tariff_dict["systemtarif"]
            
            tariff_sum = tariff_dict["tariff_sum"]
            raw_price = price - tariff_sum
            
            energinets_network_tariff = SOLAR_SELL_TARIFF["energinets_network_tariff"]
            energinets_balance_tariff = SOLAR_SELL_TARIFF["energinets_balance_tariff"]
            solar_production_seller_cut = SOLAR_SELL_TARIFF["solar_production_seller_cut"]
            
            sell_tariffs = sum((solar_production_seller_cut, energinets_network_tariff, energinets_balance_tariff, transmissions_nettarif, systemtarif))
            hour_prices[timestamp] = raw_price - sell_tariffs
    
    return hour_prices

def find_nth_local_min(
    price_dict: dict[datetime.datetime, float],
    n: int = 1,
    rise_threshold: float = 0.0,  # min. stigning efter bunden
    include_edge_last: bool = True,
):
    # Sortér efter tid, så rækkefølgen er kronologisk
    items = sorted(price_dict.items())
    times = [t for t, _ in items]
    vals = [v for _, v in items]
    minima = []

    # Find lokale minima (inkl. sidste punkt i plateau før stigning)
    for i in range(0, len(vals) - 1):
        prev_v, cur_v, next_v = vals[i - 1], vals[i], vals[i + 1]
        #_LOGGER.error(f"{times[i]} {cur_v} <= {prev_v} ({cur_v <= prev_v}) and {cur_v} < {next_v} ({cur_v < next_v})")
        if cur_v <= prev_v and (cur_v + 0) < (next_v + 0):
            minima.append(times[i])

    # Håndtér fald i slutningen som minimum
    if include_edge_last and len(vals) >= 2:
        if vals[-1] < vals[-2]:
            minima.append(times[-1])

    # Returnér n-te lokale minimum hvis det findes
    if n - 1 < len(minima):
        return minima[n - 1]
    return None

@benchmark_decorator()
def cheap_grid_charge_hours():
    func_name = "cheap_grid_charge_hours"
    func_prefix = f"{func_name}_"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    global LOCAL_ENERGY_PREDICTION_DB, CHARGING_PLAN, CHARGE_HOURS, TASKS, FORECAST_TYPE
    
    if CONFIG['prices']['entity_ids']['power_prices_entity_id'] not in state.names(domain="sensor"):
        _LOGGER.error(f"{CONFIG['prices']['entity_ids']['power_prices_entity_id']} not in entities")
        my_persistent_notification(
            i18n.t("ui.cheap_grid_charge_hours.entity_not_in_domain", entity=CONFIG['prices']['entity_ids']['power_prices_entity_id']),
            title=f"{TITLE} warning",
            persistent_notification_id=f"{__name__}_{func_name}_real_prices_not_found"
        )
        return
    
    FORECAST_TYPE = consumption_forecast_type()
    
    amount_of_days = 3
    
    current_battery_level_expenses()
    local_energy_prediction()
    
    hour_prices = deepcopy(get_hour_prices())
    sorted_by_cheapest_price = sorted(hour_prices.items(), key=lambda kv: (kv[1], kv[0]))
    
    grid_prices = deepcopy(hour_prices)
    grid_sell_prices = deepcopy(get_hour_prices(sell_prices=True))
    
    energy_prediction_db = deepcopy(LOCAL_ENERGY_PREDICTION_DB)
    battery_expenses = deepcopy(BATTERY_LEVEL_EXPENSES)

    today = getTimeStartOfDay()
    current_hour = reset_time_to_hour()
    now = getTime()
    
    chargeHours = {}
    charge_hours_alternative = {}
    
    totalCost = 0.0
    totalkWh = 0.0
    
    solar_over_production = {}
    work_overview = {}
    charging_plan = {}
    
    location = sun.get_astral_location(hass)
    
    def kwh_available_in_hour(hour):
        nonlocal func_name
        sub_func_name = "kwh_available_in_hour"
        _LOGGER = globals()['_LOGGER'].getChild(f"{func_name}.{sub_func_name}")
        
        hour_in_chargeHours = False
        kwh_available = MAX_KWH_CHARGING
        
        try:
            kwh = MAX_KWH_CHARGING
            
            percentage_missed = hour.minute / 60
            kwh_missed = kwh * percentage_missed
            
            kwh -= kwh_missed
            if hour in chargeHours:
                hour_in_chargeHours = True
                kwh_available = kwh - chargeHours[hour]['kWh']
        except Exception as e:
            _LOGGER.error(f"hour:{hour} error:{e} {type(e)}")
            my_persistent_notification(
                f"Error checking kWh available in hour:{hour} error:{e} {type(e)}",
                title=f"{TITLE} error",
                persistent_notification_id=f"{__name__}_{func_name}_{sub_func_name}_error"
            )
        return [hour_in_chargeHours, kwh_available]

    def add_to_charge_hours(kwhNeeded, totalCost, totalkWh, hour, price, kwh_available, battery_level = None, rules = []):
        nonlocal func_name
        sub_func_name = "add_to_charge_hours"
        _LOGGER = globals()['_LOGGER'].getChild(f"{func_name}.{sub_func_name}")
        
        min_charging_kwh_cron = 0.0
        min_charging_kwh_ev = 0.0
        
        try:
            min_charging_kwh_cron = CONFIG['cron_interval'] / 60 * MAX_KWH_CHARGING / 2
        except:
            pass
        
        try:
            min_charging_kwh_ev = CONFIG['solar']['powerwall_battery_size'] / 100 / 2
        except:
            pass
        
        try:
            cost = 0.0
            kwh = MAX_KWH_CHARGING
            min_charging_kwh = max(min(min_charging_kwh_cron, min_charging_kwh_ev), 0.2)
            
            battery_level_added = False
                
            price = float(price)
            kwhNeeded = float(kwhNeeded)
            
            percentage_missed = hour.minute / 60
            kwh_missed = kwh * percentage_missed
            kwh -= kwh_missed
                
            if battery_level:
                battery_level_diff = round(CONFIG['solar']['powerwall_battery_level_max'] - (battery_level + kwh_to_percentage(kwh, include_charging_loss = True)),2)
                kwh_diff = round(percentage_to_kwh(battery_level_diff, include_charging_loss = True),2)
                if kwh_diff < 0.0:
                    kwh = max(kwh + kwh_diff, 0.0)
            
            kwh = min(kwh, kwh_available)
            
            if kwh > min_charging_kwh:
                if hour not in chargeHours:
                    chargeHours[hour] = {
                        "Cost": 0.0,
                        "kWh": 0.0,
                        "battery_level": 0.0,
                        "Price": round(price, 2)
                    }
                    
                if (kwhNeeded - kwh) < 0.0:
                    kwh = kwhNeeded
                    kwhNeeded = 0.0
                else:
                    kwhNeeded = round(kwhNeeded - kwh, 2)
                    
                cost = kwh * price
                totalCost = totalCost + cost
                battery_level_added = kwh_to_percentage(kwh, include_charging_loss = True)

                chargeHours[hour]['Cost'] = round(chargeHours[hour]['Cost'] + cost, 2)
                chargeHours[hour]['kWh'] = round(chargeHours[hour]['kWh'] + kwh, 2)
                chargeHours[hour]['battery_level'] = round(chargeHours[hour]['battery_level'] + battery_level_added, 2)
                totalkWh = round(totalkWh + kwh, 2)
                
                if rules == []:
                    rules.append("no_rule")
                    
                for rule in rules:
                    if rule not in chargeHours[hour]:
                        chargeHours[hour][rule] = True
                        
                _LOGGER.info(f"Adding {hour} {price} valuta/kWh {chargeHours[hour]['battery_level']}% {chargeHours[hour]['kWh']}kWh with keys {rules}")
        except Exception as e:
            _LOGGER.error(f"Error adding to charge hours kwhNeeded:{kwhNeeded} totalCost:{totalCost} totalkWh:{totalkWh} hour:{hour} price:{price} kwh_available:{kwh_available} battery_level:{battery_level} rules:{rules} error:{e} {type(e)}")
            my_persistent_notification(
                f"Error adding to charge hours kwhNeeded:{kwhNeeded} totalCost:{totalCost} totalkWh:{totalkWh} hour:{hour} price:{price} kwh_available:{kwh_available} battery_level:{battery_level} rules:{rules} error:{e} {type(e)}",
                title=f"{TITLE} error",
                persistent_notification_id=f"{__name__}_{func_name}_{sub_func_name}_error_{hour}"
            )

        return [kwhNeeded, totalCost, totalkWh, battery_level_added, cost]

    def battery_level_flow_prediction(day, from_hour, reset_hour = False):
        nonlocal func_name
        sub_func_name = "battery_level_flow_prediction"
        _LOGGER = globals()['_LOGGER'].getChild(f"{func_name}.{sub_func_name}")
        
        nonlocal charging_plan
        
        if reset_hour:
            from_hour = max(from_hour - 1, 0)
        
        for hour in range(from_hour, 24):
            if reset_hour:
                #_LOGGER.info(f"Resetting battery level flow for day:{day} hour:{hour} battery_level before:{sum(charging_plan[day]['battery_level_flow'].get(hour, [0.0]))}")
                charging_plan[day]['battery_level_flow'][hour] = []
                
            if hour == from_hour:
                battery_level = get_battery_level() if day == 0 else sum(charging_plan[day - 1]['battery_level_flow'][23])
                
                if reset_hour:
                    charging_plan[day]['battery_level_start_of_day'] = []
                charging_plan[day]['battery_level_start_of_day'].append(battery_level)
            else:
                if hour - 1 not in charging_plan[day]['battery_level_flow']:
                    continue
                
                battery_level = sum(charging_plan[day]['battery_level_flow'][hour - 1])
                if hour == 23:
                    if reset_hour:
                        charging_plan[day]['battery_level_end_of_day'] = []
                    charging_plan[day]['battery_level_end_of_day'].append(battery_level)
                    
            if hour not in charging_plan[day]['battery_level_flow']:
                continue
            
            charging_plan[day]['battery_level_flow'][hour].append(min(max(battery_level, CONFIG['solar']['powerwall_battery_level_min']), CONFIG['solar']['powerwall_battery_level_max']))
            
            timestamp = getTimeStartOfDay() + datetime.timedelta(days=day, hours=hour)
            grid_added = charging_plan[day]['charging_sessions'].get(timestamp, {}).get('battery_level', 0.0)
            
            percentage_added = grid_added
            
            try:
                percentage_kwh = energy_prediction_db.get('solar_prediction', {}).get(day, {}).get('total', [])[hour]
                percentage_added += round(kwh_to_percentage(percentage_kwh, include_charging_loss = True), 2)
            except Exception as e:
                pass
            
            if hour not in charging_plan[day]['hour_cost_prediction'][FORECAST_TYPE]:
                continue
            
            percentage_used = charging_plan[day]['hour_cost_prediction'][FORECAST_TYPE][hour]['percentage'] * -1
                
            if timestamp not in charging_plan[day]["discharge_timestamps"]:
                percentage_used = 0.0
                
            if timestamp in charging_plan[day]["force_discharge_timestamps"]:
                percentage_used = kwh_to_percentage(charging_plan[day]["force_discharge_timestamps"][timestamp]['kwh'], include_charging_loss = True) * -1
            
            if (battery_level + percentage_added + percentage_used) < CONFIG['solar']['powerwall_battery_level_min']:
                charging_plan[day]['battery_level_flow'][hour] = [CONFIG['solar']['powerwall_battery_level_min']]
                diff = percentage_added + percentage_used
                diff_kwh = percentage_to_kwh(diff)
                
                charging_plan[day]['hour_cost_prediction'][FORECAST_TYPE][hour]['kwh_not_removed'] = diff_kwh
                percentage_used = percentage_added * -1
            elif (battery_level + percentage_added + percentage_used) > CONFIG['solar']['powerwall_battery_level_max']:
                charging_plan[day]['battery_level_flow'][hour] = [CONFIG['solar']['powerwall_battery_level_max']]
                charging_plan[day]['hour_cost_prediction'][FORECAST_TYPE][hour]['kwh_not_removed'] = 0.0
                percentage_added = 0.0
                percentage_used = 0.0
            else:
                charging_plan[day]['hour_cost_prediction'][FORECAST_TYPE][hour]['kwh_not_removed'] = 0.0
            
            charging_plan[day]['battery_level_flow'][hour].append(percentage_added)
            charging_plan[day]['battery_level_flow'][hour].append(percentage_used)
            
            if sum(charging_plan[day]['battery_level_flow'][hour]) > CONFIG['solar']['powerwall_battery_level_max']:
                charging_plan[day]['battery_level_flow'][hour] = [CONFIG['solar']['powerwall_battery_level_max']]
                
    def battery_level_flow_prediction_recalc(final_recalc=False):
        nonlocal func_name
        sub_func_name = "battery_level_flow_prediction_recalc"
        _LOGGER = globals()['_LOGGER'].getChild(f"{func_name}.{sub_func_name}")
        
        nonlocal amount_of_days, charging_plan
        
        for day in range(amount_of_days):
            from_hour = getHour() if day == 0 else 0
            battery_level_flow_prediction(day, from_hour, reset_hour = True)
            
            if final_recalc:
                for hour in range(from_hour, 24):
                    timestamp = getTimeStartOfDay() + datetime.timedelta(days=day, hours=hour)
                    
                    if hour not in charging_plan[day]['battery_level_flow']:
                        continue
                    
                    battery_level = sum(charging_plan[day]['battery_level_flow'][hour])
                    
                    if battery_level > CONFIG['solar']['powerwall_battery_level_min']:
                        continue
                    
                    if timestamp in charging_plan[day]["force_discharge_timestamps"]:
                        charging_plan[day]["force_discharge_timestamps"].pop(timestamp, None)
                        
                    if timestamp in charging_plan[day]["blocked_discharge_timestamps"]:
                        charging_plan[day]["blocked_discharge_timestamps"].pop(timestamp, None)
                        
                    if timestamp not in charging_plan[day]["discharge_timestamps"]:
                        charging_plan[day]["discharge_timestamps"].append(timestamp)
                        
    def future_charging(totalCost, totalkWh):
        nonlocal func_name
        sub_func_name = "future_charging"
        _LOGGER = globals()['_LOGGER'].getChild(f"{func_name}.{sub_func_name}")
        
        global TASKS
        nonlocal battery_expenses
        
        def bt_cell(day, hour):
            try:
                return f"{sum(charging_plan[day]['battery_level_flow'][hour]):>5.1f}%"
            except Exception as e:
                return "--%"
        
        def add_charging_session_to_day(timestamp, what_day, battery_level_id, reason = "unknown"):
            nonlocal func_name, sub_func_name
            sub_sub_func_name = "add_charging_session_to_day"
            _LOGGER = globals()['_LOGGER'].getChild(f"{func_name}.{sub_func_name}.{sub_sub_func_name}")
            
            try:
                charging_plan[what_day]['charging_sessions'][timestamp] = chargeHours[timestamp]
                charging_plan[what_day]['charging_sessions'][timestamp]['reason'] = reason
                
                if timestamp in charging_plan[what_day]["discharge_timestamps"]:
                    charging_plan[what_day]["discharge_timestamps"].remove(timestamp)
            except Exception as e:
                _LOGGER.error(f"Error in {sub_sub_func_name} what_day:{what_day} timestamp:{timestamp} battery_level_id:{battery_level_id}: {e} {type(e)}")
                my_persistent_notification(
                    f"Error in {sub_sub_func_name} what_day:{what_day} timestamp:{timestamp} battery_level_id:{battery_level_id}: {e} {type(e)}",
                    title=f"{TITLE} error",
                    persistent_notification_id=f"{__name__}_{func_name}_{sub_func_name}_{sub_sub_func_name}_error_{day}_{what_day}_timestamp_{timestamp}"
                )
                raise Exception(f"Error in {sub_sub_func_name} what_day:{what_day} timestamp:{timestamp} battery_level_id:{battery_level_id}: {e} {type(e)}")
                                
        def add_charging_to_days(timestamp, sorted_timestamp, battery_level_added):
            nonlocal func_name, sub_func_name, amount_of_days
            sub_sub_func_name = "add_charging_to_days"
            _LOGGER = globals()['_LOGGER'].getChild(f"{func_name}.{sub_func_name}.{sub_sub_func_name}")
            
            now = getTime()
            days = daysBetween(now, timestamp)
            try:
                for day in range(days, amount_of_days):
                    from_hour = timestamp.hour if day == days else 0
                    
                    battery_level_flow_prediction(day, from_hour, reset_hour = True)
                    
            except Exception as e:
                _LOGGER.error(f"Error in {sub_sub_func_name} battery_level_added:{battery_level_added}: {e} {type(e)}")
                my_persistent_notification(
                    f"Error in {sub_sub_func_name} battery_level_added:{battery_level_added}: {e} {type(e)}",
                    title=f"{TITLE} error",
                    persistent_notification_id=f"{__name__}_{func_name}_{sub_func_name}_{sub_sub_func_name}_error_{timestamp}_{sorted_timestamp}"
                )
                raise Exception(f"Error in {sub_sub_func_name} battery_level_added:{battery_level_added}: {e} {type(e)}")
        
        def cheapest_hour_fill_planner(day):
            nonlocal func_name, sub_func_name
            sub_sub_func_name = "cheapest_hour_fill_planner"
            _LOGGER = globals()['_LOGGER'].getChild(f"{func_name}.{sub_func_name}.{sub_sub_func_name}")
            
            nonlocal totalCost, totalkWh, charging_plan, chargeHours, sorted_by_cheapest_price
                        
            _LOGGER.warning(f"---------------------------------{day} cheapest_hour_fill_planner {charging_plan[day]['day_text']} {day}---------------------------------")
            
            try:
                finished = False
                
                day_prices = {}
                timestamps = []
                remove_list = []
                ignore_price_diff = get_cheap_price_period_rise_threshold()
                
                
                for timestamp, price in hour_prices.items():
                    if not in_between(timestamp, charging_plan[day]["start_of_day"], charging_plan[day]["end_of_day"]):
                        continue
                    day_prices[timestamp] = price
                
                for i in range(get_cheap_price_periods()):
                    timestamp = find_nth_local_min(day_prices, i + 1, rise_threshold=get_cheap_price_period_rise_threshold(), include_edge_last=False)
                    if timestamp is None:
                        break
                    timestamps.append((timestamp, day_prices[timestamp]))
                
                cheap_timestamps = []
                
                for timestamp, price in sorted(timestamps, key=lambda kv: (kv[1], kv[0])):
                    data = []
                    
                    for hour in range(timestamp.hour, 24):
                        loop_timestamp = timestamp.replace(hour=hour)
                        loop_price = hour_prices.get(loop_timestamp, None)
                        if loop_price is None:
                            continue
                        
                        diff = loop_price - price
                        if diff <= ignore_price_diff:
                            data.append(loop_timestamp)
                        else:
                            last_timestamp = data.pop(-1) if data else timestamp
                            timestamp = last_timestamp
                            
                            hour_in_chargeHours, kwh_available = kwh_available_in_hour(last_timestamp)
                            if hour_in_chargeHours and kwh_available <= 0.0:
                                if last_timestamp not in chargeHours:
                                    remove_list.append(last_timestamp)
                                    
                                for item in reverse_list(data):
                                    last_timestamp = item
                                    hour_in_chargeHours, kwh_available = kwh_available_in_hour(last_timestamp)
                                    if hour_in_chargeHours and kwh_available <= 0.0:
                                        if item == data[-1]:
                                            break
                                        continue
                                    timestamp = last_timestamp
                                    break
                            break
                    if timestamp not in cheap_timestamps:
                        cheap_timestamps.append(timestamp)
                
                cheapest_price = min(day_prices.values()) if day_prices else None
                cheapest_price_rise_threshold = get_cheapest_price_rise_threshold()
                
                
                for cheap_timestamp in cheap_timestamps:
                    highest_battery_level = CONFIG['solar']['powerwall_battery_level_min']
                    highest_battery_level_timestamp = cheap_timestamp
                    
                    percentage_needed_today = []
                    
                    from_hour = getHour() if day == 0 else 0
                    for hour in range(from_hour, 24):
                        percentage_needed_today.append(charging_plan[day]['hour_cost_prediction'][FORECAST_TYPE][hour]['percentage'])
                    
                    for hour in range(cheap_timestamp.hour, 24):
                        battery_level = sum(charging_plan[day]['battery_level_flow'].get(hour, [0.0]))
                        if battery_level > highest_battery_level:
                            highest_battery_level = battery_level
                            highest_battery_level_timestamp = cheap_timestamp.replace(hour=hour)
                            
                    kwh_needed = kwh_needed_for_charging(sum(percentage_needed_today), highest_battery_level)
                    #kwh_needed -= charging_plan[day]['solar_kwh_prediction_total']
                    
                    if kwh_needed <= 0.0:
                        continue
                            
                    for i in range(int(round_up(kwh_needed / MAX_KWH_CHARGING) + 1)):
                        if finished:
                            break
                        
                        timestamp = cheap_timestamp - datetime.timedelta(hours=i)
                        price = hour_prices[timestamp]
                        
                        cheapest_price_diff = price - cheapest_price if cheapest_price is not None else None
                        if cheapest_price_diff is not None and cheapest_price_diff > cheapest_price_rise_threshold:
                            continue
                        
                        if not in_between(timestamp, max(current_hour, charging_plan[day]["start_of_day"]), timestamp + datetime.timedelta(hours=1)) or timestamp < current_hour:
                            continue
                        
                        kwh_with_profit = []
                        for hour in range(timestamp.hour, 24):
                            loop_price = hour_prices.get(timestamp.replace(hour=hour), None)
                            
                            if loop_price is None:
                                continue
                            
                            profit = loop_price - (price + calc_battery_loss_cost(price) + abs(CONFIG['solar']['powerwall_wear_cost_per_kwh']))
                            min_profit_per_kwh = get_min_profit_per_kwh() if only_discharge_on_profit_enabled() else 0.0
                            
                            if profit < min_profit_per_kwh:
                                _LOGGER.info(f"Not adding hour {timestamp.replace(hour=hour)} to kwh_with_profit because profit {profit} is lower than min_profit_per_kwh {min_profit_per_kwh}")
                                continue
                            
                            kwh_with_profit.append(charging_plan[day]['hour_cost_prediction'][FORECAST_TYPE][hour]['kwh'])
                        
                        hour_in_chargeHours, kwh_available = kwh_available_in_hour(timestamp)
                        
                        kwh_available = min(sum(kwh_with_profit), percentage_to_kwh(max(CONFIG['solar']['powerwall_battery_level_max'] - highest_battery_level, 0.0), include_charging_loss = True), percentage_to_kwh(CONFIG['solar']['powerwall_battery_level_max'] - CONFIG['solar']['powerwall_battery_level_min'], include_charging_loss = True))
                        
                        
                        if hour_in_chargeHours and kwh_available <= 0.0:
                            continue
                                     
                        what_day = daysBetween(current_hour, timestamp)
                        if what_day < day - 1:
                            continue
                        
                        battery_level_id = "battery_level_start_of_day" if what_day < 0 else "battery_level_end_of_day"
                        
                        if round(kwh_needed, 1) > 0.0 and kwh_to_percentage(kwh_needed, include_charging_loss = True) > 0.0:
                            charging_plan[what_day]['rules'].append(timestamp.strftime('%d%H'))
                            kwh_needed, totalCost, totalkWh, battery_level_added, cost_added = add_to_charge_hours(kwh_needed, totalCost, totalkWh, timestamp, price, kwh_available, highest_battery_level, rules=["cheapest_hour_fill_planner"])
                            
                            if timestamp in chargeHours and battery_level_added:
                                _LOGGER.info(f"Day:{day} Added charging at hour:{timestamp} battery_level_added:{battery_level_added:.1f}% cost_added:{cost_added:.2f} valuta total_cost before:{charging_plan[day]['total_cost']:.2f}")
                                charging_plan[day]['total_cost'] += cost_added
                                reason = (
                                    f"<details><summary>{emoji_parse({'charging': True})}Billigste timer ({price}/{battery_level_added:.0f}%)</summary>"
                                    f"Højeste batteriniveau tidspunkt: **{highest_battery_level_timestamp.strftime('%H:%M')}**<br>"
                                    f"Højeste batteriniveau: **{highest_battery_level:.1f}%**<br>"
                                    f"Aktuel elpris: **{price}** valuta/kWh<br>"
                                    f"Batteriniveau tilføjet: **{battery_level_added:.2f}%**<br>"
                                    "</details>"
                                    )
                                add_charging_session_to_day(timestamp, what_day, battery_level_id, reason = reason)
                                add_charging_to_days(timestamp, highest_battery_level_timestamp, battery_level_added)
                        else:
                            finished = True
                
                """for remove_timestamp in remove_list:
                    index_of_timestamp = None
                    try:
                        index_of_timestamp = sorted_by_cheapest_price.index((remove_timestamp, hour_prices[remove_timestamp]))
                    except:
                        index_of_timestamp = None
                    if index_of_timestamp is not None:
                        sorted_by_cheapest_price.pop(sorted_by_cheapest_price.index((remove_timestamp, hour_prices[remove_timestamp])))"""
            except Exception as e:
                _LOGGER.error(f"Error in cheapest_hour_fill_planner day:{day}: {e} {type(e)}")
                save_error_to_file(f"Error in cheapest_hour_fill_planner day:{day}: {e} {type(e)}")
                my_persistent_notification(
                    f"Error in cheapest_hour_fill_planner day:{day}: {e} {type(e)}",
                    title=f"{TITLE} error",
                    persistent_notification_id=f"{__name__}_{func_name}_{sub_func_name}_{sub_sub_func_name}_error_{day}"
                )
                raise Exception(f"Error in cheapest_hour_fill_planner day:{day}: {e} {type(e)}")
            
            
            # Byg tabellen
            header_builder = []
            for i in range(amount_of_days):
                header_builder.append(f"{i:^10}")
            hdr = f"{'Hour':>10} | " + " | ".join(header_builder)
            sep   = "-" * len(hdr)
            lines = [f"After {func_name}", hdr, sep]
            
            for hour in range(24):
                string_builder = []
                for i in range(amount_of_days):
                    string_builder.append(f"{bt_cell(i, hour):^10}")
                line = f"{hour:>10} | " + " | ".join(string_builder)
                lines.append(line)

            for line in lines:
                _LOGGER.error(line)

        def most_expensive_planner(day):
            nonlocal func_name, sub_func_name
            sub_sub_func_name = "most_expensive_planner"
            _LOGGER = globals()['_LOGGER'].getChild(f"{func_name}.{sub_func_name}.{sub_sub_func_name}")
            
            nonlocal totalCost, totalkWh, charging_plan, chargeHours, sorted_by_cheapest_price
                        
            _LOGGER.warning(f"---------------------------------{day} most_expensive_planner {charging_plan[day]['day_text']} {day}---------------------------------")
            
            try:
                for sorted_hour in charging_plan[day]['sorted_hour_cost_prediction'][FORECAST_TYPE]:
                    sorted_timestamp = current_hour.replace(hour=sorted_hour) + datetime.timedelta(days=day)
                    sorted_kwh_needed = charging_plan[day]['hour_cost_prediction'][FORECAST_TYPE][sorted_hour]['kwh']
                    sorted_percentage_needed = kwh_to_percentage(sorted_kwh_needed)
                    sorted_price = charging_plan[day]['hour_cost_prediction'][FORECAST_TYPE][sorted_hour]['price']
                    
                    if sorted_timestamp <= current_hour:
                        continue
                    sorted_battery_level = sum(charging_plan.get(day, {}).get('battery_level_flow', {}).get(sorted_timestamp.hour, [0.0]))
                    
                    if sorted_percentage_needed <= sorted_battery_level:
                        continue
                    
                    remove_list = []
                    
                    for timestamp, price in sorted_by_cheapest_price:
                        if sorted_timestamp.date() != timestamp.date():
                            continue
                        
                        if not in_between(timestamp, current_hour, sorted_timestamp):
                            continue
                        
                        kwh_profit = sorted_price - (price + calc_battery_loss_cost(price) + abs(CONFIG['solar']['powerwall_wear_cost_per_kwh']))
                        min_profit_per_kwh = get_min_profit_per_kwh() if only_discharge_on_profit_enabled() else 0.0
                        
                        if kwh_profit < min_profit_per_kwh:
                            continue
                        
                        hour_in_chargeHours, kwh_available = kwh_available_in_hour(timestamp)
                        if hour_in_chargeHours and kwh_available <= 0.0:
                            remove_list.append(timestamp)
                            continue
                        
                        highest_battery_level_timestamp = timestamp
                        highest_battery_level = 0.0
                        
                        for i in range(1, hoursBetween(timestamp, sorted_timestamp) + 1):
                            loop_day = daysBetween(getTime(), sorted_timestamp - datetime.timedelta(hours=i))
                            loop_hour = (sorted_timestamp - datetime.timedelta(hours=i)).hour
                            loop_battery_level = sum(charging_plan.get(loop_day, {}).get('battery_level_flow', {}).get(loop_hour, [0.0]))
                            
                            if loop_battery_level > highest_battery_level:
                                highest_battery_level_timestamp = sorted_timestamp - datetime.timedelta(hours=i)
                                highest_battery_level = loop_battery_level
                        
                        available_battery_level = CONFIG['solar']['powerwall_battery_level_max'] - max(highest_battery_level, CONFIG['solar']['powerwall_battery_level_min'])
                        available_kwh = percentage_to_kwh(available_battery_level, include_charging_loss = True)
                        
                        if available_kwh <= 0.0:
                            remove_list.append(timestamp)
                            continue
                        
                        kwh_needed = min(sorted_kwh_needed, available_kwh)
                        #kwh_needed -= charging_plan[day]['solar_kwh_prediction_total']
                        
                        if kwh_needed <= 0.0:
                            remove_list.append(timestamp)
                            continue
                        
                        kwh_available = round(min(available_kwh, kwh_available), 1)
                        
                        
                        if kwh_available <= 0.0:
                            remove_list.append(timestamp)
                            continue
                        
                        what_day = daysBetween(getTime(), timestamp)
                        if what_day < day - 1:
                            continue
                        
                        battery_level_id = "battery_level_start_of_day" if what_day < 0 else "battery_level_end_of_day"
                        
                        if round(kwh_needed, 1) > 0.0 and kwh_to_percentage(kwh_needed, include_charging_loss = True) > 0.0:
                            charging_plan[what_day]['rules'].append(sorted_timestamp.strftime('%d%H'))
                            sorted_kwh_needed, totalCost, totalkWh, battery_level_added, cost_added = add_to_charge_hours(kwh_needed, totalCost, totalkWh, timestamp, price, kwh_available, highest_battery_level, rules=[f"most_expensive_planner"])
                            
                            if timestamp in chargeHours and battery_level_added:
                                _LOGGER.info(f"day:{day} kwh_profit:{kwh_profit} = sorted_price:{sorted_price} - (calc_battery_loss_cost({price}):{calc_battery_loss_cost(price)} + {abs(CONFIG['solar']['powerwall_wear_cost_per_kwh'])})")
                                
                                charging_plan[day]['total_cost'] += cost_added
                                reason = (
                                    f"<details><summary>{emoji_parse({'charging': True})}Dyreste timer ({price}/{battery_level_added:.0f}%)</summary>"
                                    f"Prioriteret time: **{sorted_timestamp}**<br>"
                                    f"Prioriteret batteriniveau: **{sorted_battery_level:.1f}%**<br>"
                                    f"Prioriteret elpris: **{sorted_price}** valuta/kWh<br>"
                                    f"Højeste batteriniveau tidspunkt: **{highest_battery_level_timestamp.strftime('%H:%M')}**<br>"
                                    f"Højeste batteriniveau: **{highest_battery_level:.1f}%**<br>"
                                    f"Aktuel elpris: **{price} valuta/kWh**<br>"
                                    f"Profit ved at lade til denne time: **{kwh_profit:.2f} valuta/kWh**<br>"
                                    f"Tilgængeligt batteriniveau: **{available_battery_level:.1f}%**<br>"
                                    f"Batteriniveau tilføjet: **{battery_level_added:.2f}%**<br>"
                                    f"</details>"
                                    )
                                add_charging_session_to_day(timestamp, what_day, battery_level_id, reason = reason)
                                add_charging_to_days(timestamp, sorted_timestamp, battery_level_added)
                        else:
                            break
                    
                    for remove_timestamp in remove_list:
                        sorted_by_cheapest_price.pop(sorted_by_cheapest_price.index((remove_timestamp, hour_prices[remove_timestamp])))
            except Exception as e:
                _LOGGER.error(f"Error in most_expensive_planner day:{day}: {e} {type(e)}")
                save_error_to_file(f"Error in most_expensive_planner day:{day}: {e} {type(e)}")
                my_persistent_notification(
                    f"Error in most_expensive_planner day:{day}: {e} {type(e)}",
                    title=f"{TITLE} error",
                    persistent_notification_id=f"{__name__}_{func_name}_{sub_func_name}_{sub_sub_func_name}_error_{day}"
                )
                raise Exception(f"Error in most_expensive_planner day:{day}: {e} {type(e)}")
            
            # Byg tabellen
            header_builder = []
            for i in range(amount_of_days):
                header_builder.append(f"{i:^10}")
            hdr = f"{'Hour':>10} | " + " | ".join(header_builder)
            sep   = "-" * len(hdr)
            lines = [f"After {func_name}", hdr, sep]
            
            for hour in range(24):
                string_builder = []
                for i in range(amount_of_days):
                    string_builder.append(f"{bt_cell(i, hour):^10}")
                line = f"{hour:>10} | " + " | ".join(string_builder)
                lines.append(line)

            for line in lines:
                _LOGGER.error(line)

        def discharge_amount(day):
            nonlocal func_name, sub_func_name
            sub_sub_func_name = "discharge_amount"
            _LOGGER = globals()['_LOGGER'].getChild(f"{func_name}.{sub_func_name}.{sub_sub_func_name}")
            
            nonlocal charging_plan
            
            _LOGGER.warning(f"---------------------------------{day} discharge_amount {charging_plan[day]['day_text']} {day}---------------------------------")
            
            discharge_kwh = 0.0
            last_battery_level = 0.0
            for hour in charging_plan[day]['battery_level_flow']:
                if charging_plan[day]['battery_level_flow'][hour]:
                    if sum(charging_plan[day]['battery_level_flow'][hour]) < last_battery_level:
                        discharge_kwh += percentage_to_kwh(last_battery_level - sum(charging_plan[day]['battery_level_flow'][hour]))
                    last_battery_level = sum(charging_plan[day]['battery_level_flow'][hour])

            _LOGGER.info(f"Discharge amount day:{day} discharge_kwh:{discharge_kwh}kWh")
            charging_plan[day]['discharge_kwh'] = discharge_kwh
        
        def _get_predicted_battery_cost(day):
            nonlocal func_name, sub_func_name
            sub_sub_func_name = "get_predicted_battery_cost"
            _LOGGER = globals()['_LOGGER'].getChild(f"{func_name}.{sub_func_name}.{sub_sub_func_name}")
            
            nonlocal charging_plan, grid_prices, battery_expenses
            
            total_grid_solar_kwh = []
            total_grid_cost_prediction = []
            
            if charging_plan[day]['solar_kwh_prediction_total'] is not None:
                for hour in range(current_hour.hour, 24):
                    if charging_plan[day]['solar_kwh_prediction'][hour] <= 0.0:
                        continue
                    
                    total_grid_solar_kwh.append(charging_plan[day]['solar_kwh_prediction'][hour])
                    total_grid_cost_prediction.append(charging_plan[day]['solar_cost_prediction'][hour])
                
            for timestamp, charging_session in charging_plan[day]['charging_sessions'].items():
                if timestamp >= current_hour:
                    _LOGGER.info(f"Adding charging session for day:{day} timestamp:{timestamp} kWh:{charging_session['kWh']} cost:{charging_session['Cost']}")
                    total_grid_solar_kwh.append(charging_session['kWh'])
                    total_grid_cost_prediction.append(charging_session['Cost'])
            
            if day == 0:
                powerwall_kwh = percentage_to_kwh(get_battery_level(), include_charging_loss=True)
                powerwall_kwh_price = battery_expenses.get("battery_level_expenses_unit", None)
                
                if not isinstance(powerwall_kwh_price, (int, float)):
                    powerwall_kwh_price = get_powerwall_kwh_price()
                _LOGGER.info(f"Adding powerwall kWh for day:{day} kWh:{powerwall_kwh} price:{powerwall_kwh_price}")
                total_grid_solar_kwh.append(powerwall_kwh)
                total_grid_cost_prediction.append(powerwall_kwh_price * powerwall_kwh)
            _LOGGER.warning(f"Total grid solar kWh prediction for day:{day}: {total_grid_solar_kwh} total grid cost prediction for day:{day}: {total_grid_cost_prediction}")
            battery_kwh_cost_raw = (sum(total_grid_cost_prediction)) / sum(total_grid_solar_kwh) if sum(total_grid_solar_kwh) > 0.0 else 0.0
            battery_loss_cost = calc_battery_loss_cost(battery_kwh_cost_raw)
            battery_kwh_cost = battery_kwh_cost_raw + battery_loss_cost + abs(CONFIG['solar']['powerwall_wear_cost_per_kwh'])
            _LOGGER.info(f"Predicted battery kWh cost for day:{day} battery_kwh_cost_raw:{battery_kwh_cost_raw:.2f} battery_loss_cost:{battery_loss_cost:.2f} battery_kwh_cost:{battery_kwh_cost:.2f}")
            return battery_kwh_cost_raw, battery_loss_cost, battery_kwh_cost
        
        def only_discharge_on_profit(day):
            nonlocal func_name, sub_func_name
            sub_sub_func_name = "only_discharge_on_profit"
            _LOGGER = globals()['_LOGGER'].getChild(f"{func_name}.{sub_func_name}.{sub_sub_func_name}")
            
            nonlocal charging_plan, grid_prices
            
            battery_kwh_cost_raw, battery_loss_cost, battery_kwh_cost = _get_predicted_battery_cost(day)
            
            for hour in range(24):
                if hour not in charging_plan[day]['battery_level_flow']:
                    continue
                
                if hour not in charging_plan[day]['hour_cost_prediction'][FORECAST_TYPE]:
                    continue
                
                timestamp = charging_plan[day]["start_of_day"] + datetime.timedelta(hours=hour)
                
                if timestamp not in charging_plan[day]["discharge_timestamps"]:
                    continue
                
                price = grid_prices.get(timestamp, None)
                
                if price is None:
                    continue
                
                min_profit_per_kwh = get_min_profit_per_kwh()
                profit = price - battery_kwh_cost
                
                if profit < min_profit_per_kwh:
                    if timestamp not in charging_plan[day]["discharge_timestamps"]:
                        _LOGGER.warning(f"Discharge not allowed at day:{day} hour:{hour} timestamp:{timestamp} not in discharge_timestamps, skipping")
                        continue
                    
                    charging_plan[day]["discharge_timestamps"].remove(timestamp)
                    
                    if sum(charging_plan[day]['battery_level_flow'][hour]) <= CONFIG['solar']['powerwall_battery_level_min']:
                        continue
                    
                    charging_plan[day]['battery_level_flow'][hour].append(charging_plan[day]['hour_cost_prediction'][FORECAST_TYPE][hour]['percentage'])
                    
                    reason = (
                        f"<details><summary>{emoji_parse({'blocked': True})}Ingen fortjeneste ({profit:.2f})</summary>"
                        f"Battery level: **{sum(charging_plan[day]['battery_level_flow'][hour]):.1f}%**<br>"
                        f"Battery kWh cost (basis): **{battery_kwh_cost_raw:.2f} valuta/kWh**<br>"
                        f"Charge/Discharge loss: **{battery_loss_cost:.2f} valuta/kWh**<br>"
                        f"Wear cost per kWh: **{abs(CONFIG['solar']['powerwall_wear_cost_per_kwh']):.2f} valuta/kWh**<br>"
                        f"**Samlet battery kWh cost**: **{battery_kwh_cost:.2f} valuta/kWh**<br>"
                        f"Aktuel elpris: **{price} valuta/kWh**<br>"
                        f"Minimum fortjeneste pr. kWh: **{min_profit_per_kwh:.2f} valuta/kWh**<br>"
                        f"Fortjeneste pr. kWh: **{profit:.2f} valuta/kWh**<br>"
                        "</details>"
                        )
                    charging_plan[day]['blocked_discharge_timestamps'][timestamp] = reason
            
        def prioritize_discharge_hours_by_energy_cost(day):
            nonlocal func_name, sub_func_name
            sub_sub_func_name = "prioritize_discharge_hours_by_energy_cost"
            _LOGGER = globals()['_LOGGER'].getChild(f"{func_name}.{sub_func_name}.{sub_sub_func_name}")
            
            nonlocal charging_plan, grid_prices
            
            def _hour_after_prioritized_hours(hour):
                if not prioritized_hours:
                    return False
                return hour > max(prioritized_hours)
            
            powerwall_kwh_min = percentage_to_kwh(CONFIG['solar']['powerwall_battery_level_min'])
                        
            highest_battery_level = get_battery_level() if day == 0 else CONFIG['solar']['powerwall_battery_level_min']
            highest_battery_level_timestamp = getTime() if day == 0 else charging_plan[day]['start_of_day']
            
            for hour in charging_plan[day]['hour_cost_prediction'][FORECAST_TYPE].keys():
                battery_level = sum(charging_plan[day]['battery_level_flow'].get(hour, [0.0]))
                if battery_level > highest_battery_level:
                    highest_battery_level = battery_level
                    highest_battery_level_timestamp = current_hour.replace(hour=hour) + datetime.timedelta(days=day)
            
            highest_battery_kwh = percentage_to_kwh(highest_battery_level)
            battery_kwh = highest_battery_kwh
            
            prioritized_hours = []
            prioritized_kwh = []
            
            for hour in charging_plan[day]['sorted_hour_cost_prediction'][FORECAST_TYPE]:
                timestamp = current_hour.replace(hour=hour) + datetime.timedelta(days=day)
                
                if timestamp not in charging_plan[day]["discharge_timestamps"]:
                    continue
                
                if timestamp < highest_battery_level_timestamp:
                    continue
                    
                kwh_needed = charging_plan[day]['hour_cost_prediction'][FORECAST_TYPE][hour]['kwh']
                battery_kwh -= kwh_needed
                battery_kwh = max(battery_kwh, powerwall_kwh_min)
                    
                if battery_kwh <= powerwall_kwh_min:
                    if _hour_after_prioritized_hours(hour):
                        continue
                    charging_plan[day]["discharge_timestamps"].remove(timestamp.replace(hour=hour))
                    
                    prioritized_hours_string = ", ".join([str(hour) for hour in prioritized_hours])
                    prioritized_kwh_string = f"{', '.join([f'{kwh_to_percentage(kwh):.0f}%' for kwh in prioritized_kwh])} ({kwh_to_percentage(sum(prioritized_kwh)):.1f}%)"
                    
                    reason = (
                        f"<details><summary>{emoji_parse({'blocked': True})}Dyreste timer prioriteret ({prioritized_hours_string.replace(' ', '')})</summary>"
                        "(battery_kwh ≤ minimum threshold)<br>"
                        f"Prioriterede timer: **{prioritized_hours_string}**<br>"
                        f"Prioriterede kWh: **{prioritized_kwh_string}**<br>"
                        f"Highest battery level time: **{highest_battery_level_timestamp}**<br>"
                        f"Highest battery kWh: **{kwh_to_percentage(highest_battery_kwh):.1f}%**<br>"
                        f"Current battery kWh: **{kwh_to_percentage(battery_kwh):.1f}%**<br>"
                        f"Minimum allowed kWh: **{kwh_to_percentage(powerwall_kwh_min):.1f}%**<br>"
                        f"Kwh needed for discharge: **{kwh_to_percentage(kwh_needed):.1f}%**<br>"
                        "</details>"
                        )
                    charging_plan[day]['blocked_discharge_timestamps'][timestamp] = reason
                else:
                    prioritized_hours.append(hour)
                    prioritized_kwh.append(kwh_needed)
                
        def sell_excess_kwh(day):
            nonlocal func_name, sub_func_name
            sub_sub_func_name = "sell_excess_kwh"
            _LOGGER = globals()['_LOGGER'].getChild(f"{func_name}.{sub_func_name}.{sub_sub_func_name}")
            
            nonlocal charging_plan, grid_prices, grid_sell_prices
            
            battery_level_end_of_day = max(sum(charging_plan[day]['battery_level_end_of_day']) - CONFIG['solar']['powerwall_battery_level_min'], 0.0)
            excess_kwh_available = percentage_to_kwh(battery_level_end_of_day, include_charging_loss = True)
            
            discharge_hours_needed = round_up(excess_kwh_available / (abs(CONFIG['solar']['powerwall_discharging_power']) / 1000.0))
            
            battery_kwh_cost_raw, battery_loss_cost, battery_kwh_cost = _get_predicted_battery_cost(day)
            
            grid_sell_prices_for_day = {timestamp: price for timestamp, price in grid_sell_prices.items() if charging_plan[day]["start_of_day"].date() == timestamp.date()}
            
            exclude_hours = get_exclude_sell_hours()
            
            min_profit_per_kwh = get_min_profit_per_kwh()
            
            for i, (timestamp, price) in enumerate(sorted(grid_sell_prices_for_day.items(), key=lambda kv: (kv[1], kv[0]), reverse=True)):
                
                if i >= discharge_hours_needed or excess_kwh_available <= 0.0:
                    _LOGGER.info(f"Reached discharge_hours_needed:{discharge_hours_needed} or no more excess_kwh_available:{excess_kwh_available}kWh, stopping selling excess kWh for day:{day}")
                    break
                
                if current_hour > timestamp:
                    continue
                
                if exclude_hours and timestamp.hour in exclude_hours:
                    _LOGGER.warning(f"Excluding timestamp:{timestamp} from selling excess kWh due to exclude_hours setting")
                    continue
                
                excess_kwh_available_current_hour = min(excess_kwh_available, (abs(CONFIG['solar']['powerwall_discharging_power']) / 1000.0))
                
                excess_profit = excess_kwh_available_current_hour * price
                excess_profit -= excess_kwh_available_current_hour * battery_kwh_cost
                
                kwh_profit = price - battery_kwh_cost
                min_profit_per_kwh = get_min_profit_per_kwh() if only_discharge_on_profit_enabled() else 0.0
                
                if kwh_profit < min_profit_per_kwh:
                    _LOGGER.warning(f"Day:{day} Not selling excess kWh due to kwh_profit:{kwh_profit} < {min_profit_per_kwh}")
                    continue
                
                excess_kwh_available -= excess_kwh_available_current_hour
                
                if timestamp in charging_plan[day]["discharge_timestamps"]:
                    charging_plan[day]["discharge_timestamps"].remove(timestamp)
                
                charging_plan[day]["force_discharge_timestamps"][timestamp] = {
                    "kwh": excess_kwh_available_current_hour,
                    "profit": excess_profit,
                    "reason": (
                        f"<details><summary>{emoji_parse({'discharging': True})}Sælger overskydende kWh ({excess_profit:.2f})</summary>"
                        f"Battery kWh cost (basis): **{battery_kwh_cost_raw:.2f} valuta/kWh**<br>"
                        f"Charge/Discharge loss: **{battery_loss_cost:.2f} valuta/kWh**<br>"
                        f"Wear cost per kWh: **{abs(CONFIG['solar']['powerwall_wear_cost_per_kwh']):.2f} valuta/kWh**<br>"
                        f"**Samlet battery kWh cost**: **{battery_kwh_cost:.2f} valuta/kWh**<br>"
                        f"Grid price: **{price:.2f} valuta/kWh**<br>"
                        f"Excess kWh sold: **{excess_kwh_available_current_hour:.2f} kWh**<br>"
                        f"Profit from selling: **{excess_profit:.2f} valuta**<br>"
                        "</details>"
                    ),
                    }
            
            if len(charging_plan[day]["force_discharge_timestamps"]) == 0:
                sorted_sell_prices_for_day = sorted(grid_sell_prices_for_day.items(), key=lambda kv: (kv[1], kv[0]), reverse=True)
                max_price_timestamp = sorted_sell_prices_for_day[0][0] if len(sorted_sell_prices_for_day) > 0 else None
                max_price = sorted_sell_prices_for_day[0][1] if len(sorted_sell_prices_for_day) > 0 else 0.0
                profit = max_price - battery_kwh_cost
                
                hour_excluded = ""
                
                if exclude_hours and max_price_timestamp and max_price_timestamp.hour in exclude_hours:
                    hour_excluded = f"⚠️ Maks pris tidspunktet er i de ekskluderede timer: <b>{max_price_timestamp.hour}:00</b><br>"
                
                charging_plan[day]["force_discharge_timestamps_empty"] = (
                        f"<details><summary>⚠️Ingen gode tidspunkter at sælge overskydende kWh på ({profit:.2f})</summary>"
                        f"{hour_excluded}"
                        f"Battery kWh cost (basis): <b>{battery_kwh_cost_raw:.2f} valuta/kWh</b><br>"
                        f"Charge/Discharge loss: <b>{battery_loss_cost:.2f} valuta/kWh</b><br>"
                        f"Wear cost per kWh: <b>{abs(CONFIG['solar']['powerwall_wear_cost_per_kwh']):.2f} valuta/kWh</b><br>"
                        f"<b>Samlet battery kWh cost: {battery_kwh_cost:.2f} valuta/kWh</b><br>"
                        f"Maks pris tidspunkt i dag: <b>{max_price_timestamp if max_price_timestamp else 'N/A'}</b><br>"
                        f"Maks pris for at sælge i dag: <b>{max_price:.2f} valuta/kWh</b><br>"
                        f"Minimum fortjeneste pr. kWh: <b>{min_profit_per_kwh:.2f} valuta/kWh</b><br>"
                        f"Profit from selling: <b>{profit:.2f} valuta/kWh</b><br>"
                        "</details>"
                    )
                
        charging_rules = {
            1: {
                "name": "cheapest_hour_fill_planner",
                "enabled_func": cheapest_hour_fill_planner_enabled(),
                "func": cheapest_hour_fill_planner
            },
            2: {
                "name": "most_expensive_planner",
                "enabled_func": most_expensive_planner_enabled(),
                "func": most_expensive_planner
            },
        }
        
        for day in sorted([key for key in charging_plan.keys() if isinstance(key, int)]):
            for rule_priority in sorted(charging_rules.keys()):
                if not charging_rules[rule_priority]['enabled_func']:
                    continue
                
                TASKS[f"{func_prefix}{charging_rules[rule_priority]['name']}_{day}"] = task.create(charging_rules[rule_priority]['func'], day)
                done, pending = task.wait({TASKS[f"{func_prefix}{charging_rules[rule_priority]['name']}_{day}"]})
                
            TASKS[f"{func_prefix}battery_level_flow_prediction_recalc_{day}"] = task.create(battery_level_flow_prediction_recalc)
            done, pending = task.wait({TASKS[f"{func_prefix}battery_level_flow_prediction_recalc_{day}"]})
    
            if only_discharge_on_profit_enabled():
                TASKS[f"{func_prefix}only_discharge_on_profit_{day}"] = task.create(only_discharge_on_profit, day)
                done, pending = task.wait({TASKS[f"{func_prefix}only_discharge_on_profit_{day}"]})
                
                TASKS[f"{func_prefix}battery_level_flow_prediction_recalc_{day}"] = task.create(battery_level_flow_prediction_recalc)
                done, pending = task.wait({TASKS[f"{func_prefix}battery_level_flow_prediction_recalc_{day}"]})
                
            if prioritize_discharge_hours_by_energy_cost_enabled():
                TASKS[f"{func_prefix}prioritize_discharge_hours_by_energy_cost_{day}"] = task.create(prioritize_discharge_hours_by_energy_cost, day)
                done, pending = task.wait({TASKS[f"{func_prefix}prioritize_discharge_hours_by_energy_cost_{day}"]})
            
                TASKS[f"{func_prefix}battery_level_flow_prediction_recalc_{day}"] = task.create(battery_level_flow_prediction_recalc)
                done, pending = task.wait({TASKS[f"{func_prefix}battery_level_flow_prediction_recalc_{day}"]})
                
            if sell_excess_kwh_available_enabled():
                TASKS[f"{func_prefix}sell_excess_kwh_{day}"] = task.create(sell_excess_kwh, day)
                done, pending = task.wait({TASKS[f"{func_prefix}sell_excess_kwh_{day}"]})
            
                TASKS[f"{func_prefix}battery_level_flow_prediction_recalc_{day}"] = task.create(battery_level_flow_prediction_recalc)
                done, pending = task.wait({TASKS[f"{func_prefix}battery_level_flow_prediction_recalc_{day}"]})
                        
        TASKS[f"{func_prefix}battery_level_flow_prediction_recalc_final_recalc"] = task.create(battery_level_flow_prediction_recalc, final_recalc = True)
        done, pending = task.wait({TASKS[f"{func_prefix}battery_level_flow_prediction_recalc_final_recalc"]})
            
        for day in sorted([key for key in charging_plan.keys() if isinstance(key, int)]):
            TASKS[f"{func_prefix}discharge_amount_{day}"] = task.create(discharge_amount, day)
            done, pending = task.wait({TASKS[f"{func_prefix}discharge_amount_{day}"]})
            
        return [totalCost, totalkWh]
    
    def set_min_max_battery_levels():
        return
        nonlocal func_name
        sub_func_name = "set_min_max_battery_levels"
        _LOGGER = globals()['_LOGGER'].getChild(f"{func_name}.{sub_func_name}")
        nonlocal charging_plan
        
        for day in charging_plan:
            min_battery_level = CONFIG['solar']['powerwall_battery_level_min']
            max_battery_level = CONFIG['solar']['powerwall_battery_level_max']
            
            for hour in charging_plan[day]['battery_level_flow']:
                battery_level = sum(charging_plan[day]['battery_level_flow'][hour])
                
                if battery_level < min_battery_level:
                    min_diff = min_battery_level - battery_level
                    charging_plan[day]['battery_level_flow'][hour].append(min_diff)
                    _LOGGER.warning(f"Day:{day} Hour:{hour} battery_level:{battery_level}% < min_battery_level:{min_battery_level}%, adding min_diff:{min_diff}% to battery_level_flow")
                    
                elif battery_level > max_battery_level:
                    max_diff = max_battery_level - battery_level
                    charging_plan[day]['battery_level_flow'][hour].append(max_diff)
                    _LOGGER.warning(f"Day:{day} Hour:{hour} battery_level:{battery_level}% > max_battery_level:{max_battery_level}%, adding max_diff:{max_diff}% to battery_level_flow")
                
                else:
                    charging_plan[day]['battery_level_flow'][hour].append(CONFIG['solar']['powerwall_battery_level_min'])

    for day in range(amount_of_days):
        start_of_day_datetime = getTimeStartOfDay() + datetime.timedelta(days=day)
        end_of_day_datetime = getTimeEndOfDay() + datetime.timedelta(days=day)
        day_text = getDayOfWeekText(end_of_day_datetime)
        from_hour = getHour() if day == 0 else 0
        
        charging_plan[day] = {
            "start_of_day": start_of_day_datetime,
            "end_of_day": end_of_day_datetime,
            "total_needed_battery_level": 0.0,
            "day_text": day_text,
            "label": None,
            "emoji": "",
            "rules": [],
            "hour_cost_prediction": {
                "ema": {},
                "trend": {},
                "avg": {}
            },
            "sorted_hour_cost_prediction": {
                "ema": [],
                "trend": [],
                "avg": []
            },
            "grouped_hour_cost_prediction": {
                "ema": {},
                "trend": {},
                "avg": {}
            },
            "grouped_sorted_hour_cost_prediction": {
                "ema": [],
                "trend": [],
                "avg": []
            },
            "charging_sessions": {},
            "kwh_needed": 0.0,
            "solar_prediction": [],
            "solar_kwh_prediction": [],
            "solar_cost_prediction": [],
            "solar_kwh_prediction_total": None,
            "solar_cost_prediction_avg": None,
            "solar_prediction_corrected": False,
            "battery_level_start_of_day": [],
            "battery_level_end_of_day": [],
            "battery_level_flow": {},
            "battery_level_flow_sum": {},
            "total_cost": 0.0,
            "discharge_timestamps": [],
            "force_discharge_timestamps": {},
            "blocked_discharge_timestamps": {},
        }
        
        for hour in range(from_hour, 24):
            dt = start_of_day_datetime + datetime.timedelta(hours=hour)
            values = reverse_list(get_list_values(POWER_VALUES_DB[hour].get("power_consumption_without_all_exclusion", [0.0])))
            
            ema = calculate_ema(values) / 1000
            trend = calculate_trend(values) / 1000
            avg = average(values) / 1000
            
            charging_plan[day]['hour_cost_prediction']['ema'][hour] = {
                "percentage": kwh_to_percentage(ema),
                "kwh": ema,
                "kwh_not_removed": ema,
                "price": hour_prices.get(dt, 0.0),
                "cost": hour_prices.get(dt, 0.0) * ema
            }
            charging_plan[day]['hour_cost_prediction']['trend'][hour] = {
                "percentage": kwh_to_percentage(trend),
                "kwh": trend,
                "kwh_not_removed": trend,
                "price": hour_prices.get(dt, 0.0),
                "cost": hour_prices.get(dt, 0.0) * trend
            }
            charging_plan[day]['hour_cost_prediction']['avg'][hour] = {
                "percentage": kwh_to_percentage(avg),
                "kwh": avg,
                "kwh_not_removed": avg,
                "price": hour_prices.get(dt, 0.0),
                "cost": hour_prices.get(dt, 0.0) * avg
            }
            
            charging_plan[day]["discharge_timestamps"].append(start_of_day_datetime.replace(hour=hour))
            charging_plan[day]['battery_level_flow'][hour] = []
        
        forecast_types = ['ema', 'trend', 'avg']
        for forecast_type in forecast_types:
            dict_sorted = sorted(charging_plan[day]['hour_cost_prediction'][forecast_type].items(), key=lambda kv: (kv[1]['cost'], kv[0]), reverse=True)
            
            charging_plan[day]['sorted_hour_cost_prediction'][forecast_type] = [s[0] for s in dict_sorted]
            
            grouped_dict = {}
            last_hour = -2
            dict_key = 0
            for i, sorted_hour in enumerate(charging_plan[day]['sorted_hour_cost_prediction'][forecast_type]):
                if sorted_hour != last_hour + 1:
                    if last_hour != -2:
                        grouped_dict[dict_key]["avg_price"] = average(grouped_dict[dict_key]["price"])
                        grouped_dict[dict_key]["total_cost"] = sum(grouped_dict[dict_key]["cost"])
                        grouped_dict[dict_key]["total_kwh"] = sum(grouped_dict[dict_key]["kwh"])
                        grouped_dict[dict_key]["total_kwh_not_removed"] = sum(grouped_dict[dict_key]["kwh_not_removed"])
                        
                    dict_key = sorted_hour
                    grouped_dict[dict_key] = {
                        "hours": [],
                        "kwh": [],
                        "kwh_not_removed": [],
                        "price": [],
                        "cost": [],
                        "avg_price": 0.0,
                        "total_cost": 0.0,
                        "total_kwh": 0.0,
                        "total_kwh_not_removed": 0.0
                    }
                
                grouped_dict[dict_key]['hours'].append(sorted_hour)
                grouped_dict[dict_key]['kwh'].append(charging_plan[day]['hour_cost_prediction'][forecast_type][sorted_hour]['kwh'])
                grouped_dict[dict_key]['kwh_not_removed'].append(charging_plan[day]['hour_cost_prediction'][forecast_type][sorted_hour]['kwh_not_removed'])
                grouped_dict[dict_key]['price'].append(charging_plan[day]['hour_cost_prediction'][forecast_type][sorted_hour]['price'])
                grouped_dict[dict_key]['cost'].append(charging_plan[day]['hour_cost_prediction'][forecast_type][sorted_hour]['cost'])
                
                last_hour = sorted_hour

            charging_plan[day]['grouped_hour_cost_prediction'][forecast_type] = grouped_dict
            
            dict_sorted = sorted(charging_plan[day]['grouped_hour_cost_prediction'][forecast_type].items(), key=lambda kv: (kv[1]['total_cost'], kv[0]), reverse=True)
            
            charging_plan[day]['grouped_sorted_hour_cost_prediction'][forecast_type] = [s[0] for s in dict_sorted]
        
        
        battery_level_flow_prediction(day, from_hour)
        
        charging_plan[day]['solar_kwh_prediction'] = LOCAL_ENERGY_PREDICTION_DB.get('solar_prediction', {}).get(day, {}).get('total', [])
        charging_plan[day]['solar_cost_prediction'] = LOCAL_ENERGY_PREDICTION_DB.get('solar_prediction', {}).get(day, {}).get('total_sell', [])
        
        solar_kwh_sum = sum(charging_plan[day]['solar_kwh_prediction'])
        
        max_charging_kwh = percentage_to_kwh(CONFIG['solar']['powerwall_battery_level_max'] - CONFIG['solar']['powerwall_battery_level_min'], include_charging_loss=True)
        if solar_kwh_sum > max_charging_kwh:
            charging_plan[day]['solar_prediction_corrected'] = True
            solar_kwh_sum = min(solar_kwh_sum, max_charging_kwh)
        
        solar_percentage_sum = kwh_to_percentage(solar_kwh_sum) if charging_plan[day]['solar_kwh_prediction'] else 0.0
        solar_percentage_sum = min(solar_percentage_sum, CONFIG['solar']['powerwall_battery_level_max'] - CONFIG['solar']['powerwall_battery_level_min'])
        
        charging_plan[day]['solar_kwh_prediction_total'] = solar_kwh_sum if charging_plan[day]['solar_kwh_prediction'] else None
        charging_plan[day]['solar_cost_prediction_avg'] = average(charging_plan[day]['solar_cost_prediction']) if charging_plan[day]['solar_cost_prediction'] else None
        
        sunrise = location[0].sunrise(charging_plan[day]['start_of_day']).replace(tzinfo=None)
        sunrise_text = f"{emoji_parse({'sunrise': True})}{date_to_string(date = sunrise, format = '%H:%M')}"
        sunset = location[0].sunset(charging_plan[day]['start_of_day']).replace(tzinfo=None)
        sunset_text = f"{emoji_parse({'sunset': True})}{date_to_string(date = sunset, format = '%H:%M')}"
        
        solar_over_production[day] = {
            "day": f"{i18n.t(f'ui.calendar.weekday_names.{getDayOfWeekText(charging_plan[day]['start_of_day'])}')}",
            "date": date_to_string(date = charging_plan[day]['start_of_day'], format = "%d/%m"),
            "when": f"{sunrise_text}-{sunset_text}",
            "emoji": emoji_parse({'solar': True}),
            "percentage": solar_percentage_sum,
            "kWh": solar_kwh_sum,
            "corrected": charging_plan[day]['solar_prediction_corrected'],
        }
        if not charging_plan[day]['rules']:
            charging_plan[day]['rules'].append("no_rule")
    
    def bt_cell(day, hour):
        try:
            return f"{sum(charging_plan[day]['battery_level_flow'][hour]):>5.1f}%"
        except Exception as e:
            return "--%"
        
    # Byg tabellen
    header_builder = []
    for i in range(amount_of_days):
        header_builder.append(f"{i:^10}")
    hdr = f"{'Hour':>10} | " + " | ".join(header_builder)
    sep   = "-" * len(hdr)
    lines = ["Solar only", hdr, sep]
    
    for hour in range(24):
        string_builder = []
        for i in range(amount_of_days):
            string_builder.append(f"{bt_cell(i, hour):^10}")
        line = f"{hour:>10} | " + " | ".join(string_builder)
        lines.append(line)

    for line in lines:
        _LOGGER.info(line)
            
    for day in range(amount_of_days):
        def fmt_cell(seq, i):
            try:
                hour = seq[i][0]
                kwh = seq[i][1]['kwh']
                cost = seq[i][1]['cost']
                return f"{hour:02d}: {kwh:>6.1f}kWh {cost:>6.2f}{i18n.t('ui.common.valuta')}"
            except:
                return "--: --kWh / --kr"
            
        # Byg tabellen
        title = f"📅 {day} {charging_plan[day]['day_text']} {date_to_string(charging_plan[day]['start_of_day'], format='%d/%m')}"
        hdr   = f"{'Idx':>3} | {'EMA (hour / kwh / cost)':^30} | {'Trend (hour / kwh / cost)':^30} | {'Avg (hour / kwh / cost)':^30}"
        sep   = "-" * len(hdr)
        lines = [title, hdr, sep]

        ema_sorted = sorted(charging_plan[day]['hour_cost_prediction']['ema'].items(), key=lambda kv: (kv[1]['cost'], kv[0]), reverse=True)
        trend_sorted = sorted(charging_plan[day]['hour_cost_prediction']['trend'].items(), key=lambda kv: (kv[1]['cost'], kv[0]), reverse=True)
        avg_sorted = sorted(charging_plan[day]['hour_cost_prediction']['avg'].items(), key=lambda kv: (kv[1]['cost'], kv[0]), reverse=True)
                
        for i in range(len(ema_sorted)):
            line = f"{i+1:>3} | {fmt_cell(ema_sorted, i):<30} | {fmt_cell(trend_sorted, i):<30} | {fmt_cell(avg_sorted, i):<30}"
            lines.append(line)

        for line in lines:
            _LOGGER.warning(line) if "📅" in line else _LOGGER.info(line)

    for timestamp, price in dict(deepcopy(sorted_by_cheapest_price)).items():
        if daysBetween(current_hour, timestamp) >= amount_of_days:
            sorted_by_cheapest_price.pop(sorted_by_cheapest_price.index((timestamp, price)))
    
    try:
        TASKS[f"{func_prefix}future_charging"] = task.create(future_charging, totalCost, totalkWh)
        done, pending = task.wait({TASKS[f"{func_prefix}future_charging"]})
        
        TASKS[f"{func_prefix}set_min_max_battery_levels"] = task.create(set_min_max_battery_levels)
        done, pending = task.wait({TASKS[f"{func_prefix}set_min_max_battery_levels"]})
        
        totalCost, totalkWh = TASKS[f"{func_prefix}future_charging"].result()
        """except asyncio.exceptions.InvalidStateError as e:
            _LOGGER.info(f"InvalidStateError in future_charging: TASKS[f'{func_prefix}future_charging']={TASKS[f'{func_prefix}future_charging']} {e} {type(e)}")"""
    except (asyncio.CancelledError, asyncio.TimeoutError, KeyError) as e:
        _LOGGER.warning(f"Cancelled/Timeout/KeyError in future_charging: TASKS[f'{func_prefix}future_charging']={TASKS[f'{func_prefix}future_charging']} {e} {type(e)}")
        return
    except Exception as e:
        _LOGGER.error(f"Error in future_charging: {e} {type(e)}")
        raise Exception(f"Error in future_charging: {e} {type(e)}")
    finally:
        task_cancel(func_prefix, task_remove=True, startswith=True)
    
    def bt_cell(day, hour):
        try:
            return f"{sum(charging_plan[day]['battery_level_flow'][hour]):>5.1f}%"
        except Exception as e:
            return "--%"
        
    # Byg tabellen
    header_builder = []
    for i in range(amount_of_days):
        header_builder.append(f"{i:^10}")
    hdr = f"{'Hour':>10} | " + " | ".join(header_builder)
    sep   = "-" * len(hdr)
    lines = ["Solar + scheduled", hdr, sep]
    
    for hour in range(24):
        string_builder = []
        for i in range(amount_of_days):
            string_builder.append(f"{bt_cell(i, hour):^10}")
        line = f"{hour:>10} | " + " | ".join(string_builder)
        lines.append(line)

    for line in lines:
        _LOGGER.info(line)
    
    chargeHours['total_cost'] = totalCost
    chargeHours['total_kwh'] = totalkWh
    chargeHours['total_procent'] = round(kwh_to_percentage(totalkWh, include_charging_loss = True), 2)
    
    CHARGING_PLAN = charging_plan
    CHARGE_HOURS = chargeHours
    
    charging_plan['battery_level_flow_sum'] = []
    
    for day in charging_plan.keys():
        if not isinstance(day, int):
            continue
        
        for hour in range(24):
            if hour not in charging_plan[day]['battery_level_flow']:
                continue
            timestamp = charging_plan[day]['start_of_day'] + datetime.timedelta(hours=hour)
            battery_level = round(sum(charging_plan[day]['battery_level_flow'][hour]), 1)
            charging_plan[day]['battery_level_flow_sum'][hour] = battery_level
            charging_plan['battery_level_flow_sum'].append({"hour": timestamp, "battery_level": battery_level})
        
    set_attr(f"sensor.{__name__}_powerwall_action.battery_level_flow", charging_plan['battery_level_flow_sum'])
    
    charge_timestamps = []
    discharge_timestamps = []
    force_discharge_timestamps = []
    stopped_timestamps = []
    
    for day in charging_plan.keys():
        if not isinstance(day, int):
            continue
        
        charge_timestamps.extend(charging_plan[day]["charging_sessions"].keys())
        discharge_timestamps.extend(charging_plan[day]["discharge_timestamps"])
        force_discharge_timestamps.extend(list(charging_plan[day]["force_discharge_timestamps"].keys()))
        
        for hour in range(24):
            if hour not in charging_plan[day]['battery_level_flow']:
                continue
            
            timestamp = charging_plan[day]['start_of_day'] + datetime.timedelta(hours=hour)
            
            if (timestamp in charge_timestamps or
                timestamp in discharge_timestamps or
                timestamp in force_discharge_timestamps):
                continue
            
            stopped_timestamps.append(timestamp)
        
    set_attr(f"sensor.{__name__}_powerwall_action.charge_timestamps", charge_timestamps)
    set_attr(f"sensor.{__name__}_powerwall_action.discharge_timestamps", discharge_timestamps)
    set_attr(f"sensor.{__name__}_powerwall_action.force_discharge_timestamps", force_discharge_timestamps)
    set_attr(f"sensor.{__name__}_powerwall_action.stopped_timestamps", stopped_timestamps)
            
    hour_cost_prediction_avg_dict = {}
    hour_cost_prediction_ema_dict = {}
    hour_cost_prediction_trend_dict = {}
    hour_cost_prediction_sum_avg_dict = {}
    hour_cost_prediction_sum_ema_dict = {}
    hour_cost_prediction_sum_trend_dict = {}
    
    
    for day in charging_plan.keys():
        if not isinstance(day, int):
            _LOGGER.info(f"Skipping non-int day in discharge_timestamps: {day}")
            continue
        
        start_of_day = charging_plan[day]['start_of_day']
        hour_cost_prediction = charging_plan[day]['hour_cost_prediction']
        
        sum_avg = 0.0
        sum_ema = 0.0
        sum_trend = 0.0
        
        for hour in hour_cost_prediction['avg'].keys():
            timestamp = start_of_day + datetime.timedelta(hours=hour)
            
            avg = hour_cost_prediction['avg'][hour]['kwh']
            ema = hour_cost_prediction['ema'][hour]['kwh']
            trend = hour_cost_prediction['trend'][hour]['kwh']
            sum_avg += avg
            sum_ema += ema
            sum_trend += trend
            
            hour_cost_prediction_avg_dict[timestamp] = round(avg, 2)
            hour_cost_prediction_ema_dict[timestamp] = round(ema, 2)
            hour_cost_prediction_trend_dict[timestamp] = round(trend, 2)
            hour_cost_prediction_sum_avg_dict[timestamp] = round(sum_avg, 2)
            hour_cost_prediction_sum_ema_dict[timestamp] = round(sum_ema, 2)
            hour_cost_prediction_sum_trend_dict[timestamp] = round(sum_trend, 2)
            
    set_attr(f"sensor.{__name__}_powerwall_action.hour_cost_prediction_avg", hour_cost_prediction_avg_dict)
    set_attr(f"sensor.{__name__}_powerwall_action.hour_cost_prediction_ema", hour_cost_prediction_ema_dict)
    set_attr(f"sensor.{__name__}_powerwall_action.hour_cost_prediction_trend", hour_cost_prediction_trend_dict)
    set_attr(f"sensor.{__name__}_powerwall_action.hour_cost_prediction_sum_avg", hour_cost_prediction_sum_avg_dict)
    set_attr(f"sensor.{__name__}_powerwall_action.hour_cost_prediction_sum_ema", hour_cost_prediction_sum_ema_dict)
    set_attr(f"sensor.{__name__}_powerwall_action.hour_cost_prediction_sum_trend", hour_cost_prediction_sum_trend_dict)
    
    _LOGGER.debug(f"charging_plan:\n{pformat(charging_plan, width=200, compact=True)}")
    _LOGGER.debug(f"chargeHours:\n{pformat(chargeHours, width=200, compact=True)}")
    
    old_charge_hours = deepcopy(CHARGE_HOURS)
    
    CHARGING_PLAN = charging_plan
    CHARGE_HOURS = chargeHours
    
    if check_next_24_hours_diff(old_charge_hours, chargeHours) and old_charge_hours:
        append_overview_output("Charging plan changed, next 24 hours updated")
        
    overview = []
    
    try:
        percentage = round(battery_expenses.get("battery_level_expenses_percentage", 0.0), 0)
        solar_percentage = round(battery_expenses.get("battery_level_expenses_solar_percentage", 0.0), 0)
        kwh = round(battery_expenses.get("battery_level_expenses_kwh", 0.0), 1)
        solar_kwh = round(percentage_to_kwh(battery_expenses.get("battery_level_expenses_solar_percentage", 0.0)), 1)
        cost_loss = round(battery_expenses.get('battery_level_expenses_cost_loss', 0.0), 2)
        cost_wear = round(battery_expenses.get('battery_level_expenses_cost_wear', 0.0), 2)
        cost = round(battery_expenses.get("battery_level_expenses_cost", 0.0), 2)
        unit = round(battery_expenses.get('battery_level_expenses_unit', 0.0), 2)
        unit_percentage = round(kwh_to_percentage(percentage_to_kwh(battery_expenses.get('battery_level_expenses_unit', 0.0))), 2)
        cost_with_loss = round(battery_expenses.get("battery_level_expenses_cost_with_loss_and_wear", 0.0), 2)
        unit_with_loss = round(battery_expenses.get('battery_level_expenses_unit_with_loss_and_wear', 0.0), 2)
        unit_percentage_with_loss = round(kwh_to_percentage(percentage_to_kwh(battery_expenses.get('battery_level_expenses_unit_with_loss_and_wear', 0.0)), include_charging_loss=True), 2)
        
        unit_valuta_kwh_with_loss_text = f"**{unit_with_loss:.2f} {i18n.t('ui.common.valuta_kwh')}**"
        unit_valuta_percentage_with_loss_text = f"<br>**{unit_percentage_with_loss:.2f} {i18n.t('ui.common.valuta_percentage')}**" if unit_with_loss != unit_percentage_with_loss else ""
        
        unit_valuta_kwh_text = f"<br>(**{unit:.2f} {i18n.t('ui.common.valuta_kwh')}**)"
        unit_valuta_percentage_text = f"<br>(**{unit_percentage:.2f} {i18n.t('ui.common.valuta_percentage')}**)" if unit != unit_percentage else ""
        
        cost_loss_kwh = round(cost_loss / kwh, 2) if kwh else 0.0
        cost_wear_kwh = round(cost_wear / kwh, 2) if kwh else 0.0
        
        
        if kwh > 0.0:
            overview.append("<center>\n")
            overview.append(f"## 🔎 {i18n.t('ui.cheap_grid_charge_hours.battery_level_expenses.title')} ##")
            overview.append("|  |  |")
            overview.append("|:---|---:|")
            overview.append(f"| **🔋 {i18n.t('ui.cheap_grid_charge_hours.battery_level_expenses.current_battery_level')}** | **{percentage:.0f}% {kwh:.1f} kWh** |")
            
            if solar_percentage > 0.0:
                overview.append(f"| **☀️ {i18n.t('ui.cheap_grid_charge_hours.battery_level_expenses.solar_share')}** | **{solar_percentage:.0f}% {solar_kwh:.1f} kWh** |")

            overview.append(f"| **💰 {i18n.t('ui.cheap_grid_charge_hours.battery_level_expenses.expense')}** | **{cost_with_loss:.2f} {i18n.t('ui.common.valuta')}**<br>(**{cost:.2f} {i18n.t('ui.common.valuta')}**) |")
            overview.append(f"| **📊 {i18n.t('ui.cheap_grid_charge_hours.battery_level_expenses.estimated_loss')}** | **{cost_loss:.2f} {i18n.t('ui.common.valuta')}**<br>(**{cost_loss_kwh:.2f} {i18n.t('ui.common.valuta_kwh')}**) |")
            overview.append(f"| **🛠️ {i18n.t('ui.cheap_grid_charge_hours.battery_level_expenses.estimated_wear_cost')}** | **{cost_wear:.2f} {i18n.t('ui.common.valuta')}**<br>(**{cost_wear_kwh:.2f} {i18n.t('ui.common.valuta_kwh')}**) |")
            overview.append(f"| **🧮 {i18n.t('ui.cheap_grid_charge_hours.battery_level_expenses.unit_price')}** | {unit_valuta_kwh_with_loss_text}{unit_valuta_percentage_with_loss_text}{unit_valuta_kwh_text}{unit_valuta_percentage_text} |")
            overview.append("</center>\n")
            overview.append("***")
    except Exception as e:
        _LOGGER.error(f"Failed to calculate battery level cost: {e} {type(e)}")
        
    try:
        sorted_charge_hours = sorted(
            {k: v for k, v in chargeHours.items() if isinstance(k, datetime.datetime)}.items(),
            key=lambda kv: kv[0]
        )
        
        merged_intervals = []
        has_combined = False
        current_interval = None
        
        for timestamp, value in sorted_charge_hours:
            if current_interval is None:
                current_interval = {
                    "start": timestamp,
                    "end": timestamp,
                    "type": emoji_parse(value),
                    "percentage": value['battery_level'],
                    "kWh": round(value['kWh'], 2),
                    "cost": round(value['Cost'], 2),
                    "unit": round(value['Price'], 2),
                }
            else:
                if timestamp == current_interval["end"].replace(minute=0) + datetime.timedelta(hours=1) and daysBetween(current_interval["start"], timestamp) == 0:
                    has_combined = True
                    current_interval["end"] = timestamp
                    current_interval["type"] = join_unique_emojis(current_interval["type"], emoji_parse(value))
                    current_interval["percentage"] += value['battery_level']
                    current_interval["kWh"] += round(value['kWh'], 2)
                    current_interval["cost"] += round(value['Cost'], 2)
                else:
                    merged_intervals.append(current_interval)
                    current_interval = {
                        "start": timestamp,
                        "end": timestamp,
                        "type": emoji_parse(value),
                        "percentage": value['battery_level'],
                        "kWh": round(value['kWh'], 2),
                        "cost": round(value['Cost'], 2),
                        "unit": round(value['Price'], 2),
                    }
        
        if current_interval:
            merged_intervals.append(current_interval)
        
        overview.append("<center>\n")
        overview.append(f"## 📖 {i18n.t('ui.cheap_grid_charge_hours.charging_overview_title')} ##")
        _LOGGER.warning(f"merged_intervals: {merged_intervals}")
        if merged_intervals:
            overview.append(f"|  | {i18n.t('ui.common.time')} | % | kWh | {i18n.t('ui.common.valuta_kwh')} | {i18n.t('ui.common.price')} |")
            overview.append("|---:|:---:|---:|---:|:---:|---:|")
            
            for d in merged_intervals:
                if d['start'].strftime('%H') == d['end'].strftime('%H'):
                    time_range = f"{d['start'].strftime('%d/%m %H:%M')}"
                else:
                    time_range = f"{d['start'].strftime('%d/%m %H')}-{d['end'].strftime('%H:%M')}"
                
                overview.append(f"| {emoji_text_format(d['type'], group_size=3)} | **{time_range}** | **{int(round(d['percentage'], 0))}** | **{d['kWh']:.2f}** | **{d['unit']:.2f}** | **{d['cost']:.2f}** |")
            
            if has_combined:
                overview.append(f"\n<details><summary><b>{i18n.t('ui.common.total')} {int(round(chargeHours['total_procent'],0))}% {chargeHours['total_kwh']} kWh {chargeHours['total_cost']:.2f}{i18n.t('ui.common.valuta')} ({round(chargeHours['total_cost'] / chargeHours['total_kwh'],2)} {i18n.t('ui.common.valuta_kwh')})</b></summary>")
                overview.append(f"\n|  | {i18n.t('ui.common.time')} | % | kWh | {i18n.t('ui.common.valuta_kwh')} | {i18n.t('ui.common.price')} |")
                overview.append("|---:|:---:|---:|---:|:---:|---:|")
                
                for timestamp, value in sorted_charge_hours:
                    overview.append(f"| {emoji_parse(value)} | **{timestamp.strftime('%d/%m %H:%M')}** | **{int(round(value['battery_level'], 0))}** | **{round(value['kWh'], 2):.2f}** | **{round(value['Price'], 2):.2f}** | **{round(value['Cost'], 2):.2f}** |")
                                    
                overview.append("</details>\n")
            else:
                overview.append(f"\n**{i18n.t('ui.common.total')} {int(round(chargeHours['total_procent'],0))}% {chargeHours['total_kwh']} kWh {chargeHours['total_cost']:.2f}{i18n.t('ui.common.valuta')} ({round(chargeHours['total_cost'] / chargeHours['total_kwh'],2)} {i18n.t('ui.common.valuta_kwh')})**")
                            
            if "using_offline_prices" in hour_prices and hour_prices['using_offline_prices']:
                def _build_header(n_pairs=4):
                    heads, aligns = [], []
                    for _ in range(n_pairs):
                        heads += [i18n.t('ui.common.time'), i18n.t('ui.common.price')]
                        aligns += [":---:", "---:"]
                    header = "| " + " | ".join(heads) + " |"
                    align  = "| " + " | ".join(aligns) + " |"
                    return header + "\n" + align

                def _row_from_slice(slice_pairs, n_pairs=4):
                    row_cells = []
                    
                    while len(slice_pairs) < n_pairs:
                        slice_pairs.append(("&nbsp;", "&nbsp;"))
                    for t, p in slice_pairs:
                        row_cells += [f"**{t}**", f"*{p}*"]
                    return "| " + " | ".join(row_cells) + " |"

                overview.append(f"\n\n<details><summary><b>{i18n.t('ui.cheap_grid_charge_hours.offline_prices')}!!!</b></summary>\n")

                by_day = defaultdict(list)
                for ts, price in sorted(hour_prices["missing_hours"].items(), key=lambda kv: kv[0]):
                    by_day[ts.date()].append((ts.strftime("%H:%M"), f"{price:.2f}{i18n.t('ui.common.valuta')}"))
                    
                N_PAIRS = 4

                for day in sorted(by_day.keys()):
                    rows = by_day[day]
                    overview.append(f"\n<b>{day.strftime('%d/%m')}</b>\n")

                    header = _build_header(n_pairs=N_PAIRS)
                    overview.append(header)
                    
                    for i in range(0, len(rows), N_PAIRS):
                        slice_pairs = rows[i:i+N_PAIRS]
                        md_row = _row_from_slice(slice_pairs[:], n_pairs=N_PAIRS)
                        overview.append(md_row)
                    overview.append("")

                overview.append("\n</details>\n\n")
            
            if work_overview:
                overview.append("***")
        else:
            overview.append(f"**{i18n.t('ui.cheap_grid_charge_hours.no_upcoming_charging_planned')}**")
            
            if work_overview and is_solar_configured():
                overview.append(f"**{i18n.t('ui.cheap_grid_charge_hours.enough_solar_production')}**")
        
        overview.append("</center>\n")
    except Exception as e:
        _LOGGER.error(f"Failed to create charging plan overview: {e} {type(e)}")
        _LOGGER.error(f"charging_plan:\n{pformat(charging_plan, width=200, compact=True)}")
        _LOGGER.error(f"chargeHours:\n{pformat(chargeHours, width=200, compact=True)}")
    
    try:
        if is_solar_configured():
            overview.append("<center>\n")
            
            if solar_over_production:
                overview.append(f"## 🌞 {i18n.t('ui.cheap_grid_charge_hours.solar_production_title')} ##")
                
                overview.append(f"| {i18n.t('ui.common.time')} |  |  |  | % |  | kWh |")
                overview.append("|---|---:|:---|---:|---:|---|---:|")
                
                for d in solar_over_production.values():
                    d['day'] = f"**{d['day']}**" if d['day'] else ""
                    d['date'] = f"**{d['date']}**" if d['date'] else ""
                    d['when'] = f"**{d['when']}**" if d['when'] else ""
                    d['emoji'] = f"**{emoji_text_format(d['emoji'])}**" if d['emoji'] else ""
                    d['percentage'] = f"**{round(d['percentage'], 1)}**" if d['percentage'] else "**0**"
                    d['kWh'] = f"**{round(d['kWh'], 1)}**" if d['kWh'] else "**0.0**"
                    
                    if d['corrected']:
                        d['emoji'] = emoji_parse({'solar_corrected': True})
                        
                    overview.append(f"| {d['day']} | {d['date']} | {d['when']} | {d['emoji']} | {d['percentage']} |  | {d['kWh']} |")
            else:
                overview.append(f"### 🌞 {i18n.t('ui.cheap_grid_charge_hours.no_solar_production')} ##")
                
            overview.append("</center>\n")
    except Exception as e:
        _LOGGER.error(f"Failed to create solar over production overview: {e} {type(e)}")
        _LOGGER.error(f"solar_over_production: {solar_over_production}")
        _LOGGER.error(f"charging_plan:\n{pformat(charging_plan, width=200, compact=True)}")
        _LOGGER.error(f"chargeHours:\n{pformat(chargeHours, width=200, compact=True)}")
        
    try:
        overview.append("<center>\n")
        overview.append(f"## 🔎 Hour rules ##")
        
        for day in charging_plan.keys():
            if not isinstance(day, int):
                continue
        
            dict_timestamps_joined = {}
            dict_timestamps_joined.update(charging_plan[day]['charging_sessions'])
            dict_timestamps_joined.update(charging_plan[day]['force_discharge_timestamps'])
            dict_timestamps_joined.update(charging_plan[day]['blocked_discharge_timestamps'])
            
            overview.append(f"#### 🔎 Day {day}\n")
            if len(charging_plan[day]['charging_sessions']) == 0:
                overview.append(f"{i18n.t('ui.cheap_grid_charge_hours.no_charging_sessions')}<br>\n")
                    
            if len(charging_plan[day]['blocked_discharge_timestamps']) == 0:
                overview.append(f"{i18n.t('ui.cheap_grid_charge_hours.no_blocked_discharging_planned')}<br>\n")
            
            if sell_excess_kwh_available_enabled() and len(charging_plan[day]['force_discharge_timestamps']) == 0:
                if "force_discharge_timestamps_empty" in charging_plan[day]:
                    overview.append(f"{charging_plan[day]['force_discharge_timestamps_empty']}<br>\n")
            
            if not dict_timestamps_joined:
                continue
            
            overview.append(f"| {i18n.t('ui.common.time')} | {i18n.t('ui.common.reason')} |")
            overview.append("|:---|:---|")
            
            for timestamp, data in sorted(dict_timestamps_joined.items(), key=lambda kv: kv[0]):
                overview.append(f"| **{timestamp.strftime('%H:%M')}** | {data['reason'] if isinstance(data, dict) and 'reason' in data else data} |")
                
            overview.append("\n")
            
        overview.append("</center>\n")
        overview.append("***")
    except Exception as e:
        _LOGGER.error(f"Hour rules overview failed: {e} {type(e)}")
    
    try:
        overview.append("<center>\n")
        overview.append(f"## 🔎 Group sorted Hour cost prediction ##")
        overview.append(f"### 🔎 Prediction type <font color=red>EMA</font>/<font color=yellow>Trend</font>/<font color=green>Avg</font> ###")
        
        for day in charging_plan.keys():
            if not isinstance(day, int):
                continue
            
            ema = ", ".join([f"{hour:02d}" for hour in charging_plan[day]['grouped_sorted_hour_cost_prediction']['ema']])
            trend = ", ".join([f"{hour:02d}" for hour in charging_plan[day]['grouped_sorted_hour_cost_prediction']['trend']])
            avg = ", ".join([f"{hour:02d}" for hour in charging_plan[day]['grouped_sorted_hour_cost_prediction']['avg']])
            
            overview.append("<details>\n")
            overview.append(f"<summary>🔎 Day {day}</summary>\n")
            overview.append(f"|  |  |")
            overview.append("|:---|---:|")
            overview.append(f"| <font color=red>EMA</font> | <font color=red>{ema}</font> |")
            overview.append(f"| <font color=yellow>Trend</font> | <font color=yellow>{trend}</font> |")
            overview.append(f"| <font color=green>Avg</font> | <font color=green>{avg}</font> |\n")
            overview.append("</details>\n")
            
        overview.append("</center>\n")
        overview.append("***")
    except Exception as e:
        _LOGGER.error(f"Group sorted Hour cost prediction failed: {e} {type(e)}")
    
    try:
        overview.append("<center>\n")
        overview.append(f"## 🔎 Group hour cost prediction ##")
        overview.append(f"### 🔎 Prediction type <font color=red>EMA</font>/<font color=yellow>Trend</font>/<font color=green>Avg</font> ###")
        
        last_battery_level = get_battery_level()
        for day in charging_plan.keys():
            if not isinstance(day, int):
                continue
            
            overview.append("<details>\n")
            overview.append(f"<summary>🔎 Day {day} (discharge kwh: <b>{charging_plan[day]['discharge_kwh']:.1f}kWh</b>)</summary>\n")
            
            overview.append("| First | Hours | kwh<br>needed | kwh<br>not removed | price | cost |")
            overview.append("|:---:|:---:|:---:|:---:|:---:|:---:|")
            for hour in charging_plan[day]['grouped_sorted_hour_cost_prediction'][FORECAST_TYPE]:
                hours = charging_plan[day]['grouped_hour_cost_prediction'][FORECAST_TYPE][hour]['hours']
                kwh = charging_plan[day]['grouped_hour_cost_prediction'][FORECAST_TYPE][hour]['total_kwh']
                kwh_not_removed = charging_plan[day]['grouped_hour_cost_prediction'][FORECAST_TYPE][hour]['total_kwh_not_removed']
                cost = charging_plan[day]['grouped_hour_cost_prediction'][FORECAST_TYPE][hour]['total_cost']
                
                price = charging_plan[day]['grouped_hour_cost_prediction'][FORECAST_TYPE][hour]['avg_price']
                
                hour_string = f"<font color=red>{hour:02d}</font>"
                hours_string = "<font color=red>" + ", ".join([f"{h:02d}" for h in hours]) + "</font>"
                kwh_string = f"<font color=red>{kwh:.1f}</font>" if kwh > 0.0 else f""
                kwh_not_removed_string = f"<font color=red>{kwh_not_removed:.1f}</font>" if kwh_not_removed > 0.0 else f""
                price_string = f"{price:.2f}"
                cost_string = f"<font color=red>{cost:.2f}</font>"
                
                overview.append(f"| **{hour_string}** | {hours_string} | {kwh_string} | {kwh_not_removed_string} | {price_string} | {cost_string} |")
                
            for hour in charging_plan[day]['grouped_sorted_hour_cost_prediction']["trend"]:
                hours = charging_plan[day]['grouped_hour_cost_prediction']["trend"][hour]['hours']
                kwh = charging_plan[day]['grouped_hour_cost_prediction']["trend"][hour]['total_kwh']
                kwh_not_removed = charging_plan[day]['grouped_hour_cost_prediction']["trend"][hour]['total_kwh_not_removed']
                cost = charging_plan[day]['grouped_hour_cost_prediction']["trend"][hour]['total_cost']
                
                price = charging_plan[day]['grouped_hour_cost_prediction']["trend"][hour]['avg_price']
                
                hour_string = f"<font color=yellow>{hour:02d}</font>"
                hours_string = "<font color=yellow>" + ", ".join([f"{h:02d}" for h in hours]) + "</font>"
                kwh_string = f"<font color=yellow>{kwh:.1f}</font>" if kwh > 0.0 else f""
                kwh_not_removed_string = f"<font color=yellow>{kwh_not_removed:.1f}</font>" if kwh_not_removed > 0.0 else f""
                price_string = f"{price:.2f}"
                cost_string = f"<font color=yellow>{cost:.2f}</font>"
                
                overview.append(f"| **{hour_string}** | {hours_string} | {kwh_string} | {kwh_not_removed_string} | {price_string} | {cost_string} |")
                
            for hour in charging_plan[day]['grouped_sorted_hour_cost_prediction']["avg"]:
                hours = charging_plan[day]['grouped_hour_cost_prediction']["avg"][hour]['hours']
                kwh = charging_plan[day]['grouped_hour_cost_prediction']["avg"][hour]['total_kwh']
                kwh_not_removed = charging_plan[day]['grouped_hour_cost_prediction']["avg"][hour]['total_kwh_not_removed']
                cost = charging_plan[day]['grouped_hour_cost_prediction']["avg"][hour]['total_cost']
                
                price = charging_plan[day]['grouped_hour_cost_prediction']["avg"][hour]['avg_price']
                
                hour_string = f"<font color=green>{hour:02d}</font>"
                hours_string = "<font color=green>" + ", ".join([f"{h:02d}" for h in hours]) + "</font>"
                kwh_string = f"<font color=green>{kwh:.1f}</font>" if kwh > 0.0 else f""
                kwh_not_removed_string = f"<font color=green>{kwh_not_removed:.1f}</font>" if kwh_not_removed > 0.0 else f""
                price_string = f"{price:.2f}"
                cost_string = f"<font color=green>{cost:.2f}</font>"
                
                overview.append(f"| **{hour_string}** | {hours_string} | {kwh_string} | {kwh_not_removed_string} | {price_string} | {cost_string} |")
                
            overview.append("</details>\n")
                    
        overview.append("</center>\n")
        overview.append("***")
    except Exception as e:
        _LOGGER.error(f"Group hour cost prediction failed: {e} {type(e)}")
    
    try:
        overview.append("<center>\n")
        overview.append(f"## 🔎 Sorted Hour cost prediction ##")
        overview.append(f"### 🔎 Prediction type <font color=red>EMA</font>/<font color=yellow>Trend</font>/<font color=green>Avg</font> ###")
        
        for day in charging_plan.keys():
            if not isinstance(day, int):
                continue
            
            ema = ", ".join([f"{hour:02d}" for hour in charging_plan[day]['sorted_hour_cost_prediction']['ema']])
            trend = ", ".join([f"{hour:02d}" for hour in charging_plan[day]['sorted_hour_cost_prediction']['trend']])
            avg = ", ".join([f"{hour:02d}" for hour in charging_plan[day]['sorted_hour_cost_prediction']['avg']])
            
            overview.append("<details>\n")
            overview.append(f"<summary>🔎 Day {day}</summary>\n")
            overview.append(f"|  |  |")
            overview.append("|:---|---:|")
            overview.append(f"| <font color=red>EMA</font> | <font color=red>{ema}</font> |")
            overview.append(f"| <font color=yellow>Trend</font> | <font color=yellow>{trend}</font> |")
            overview.append(f"| <font color=green>Avg</font> | <font color=green>{avg}</font> |\n")
            overview.append("</details>\n")
            
        overview.append("</center>\n")
        overview.append("***")
    except Exception as e:
        _LOGGER.error(f"Sorted Hour cost prediction failed: {e} {type(e)}")
    
    try:
        overview.append("<center>\n")
        overview.append(f"## 🔎 Hour cost prediction ##")
        overview.append(f"### 🔎 Prediction type <font color=red>EMA</font>/<font color=yellow>Trend</font>/<font color=green>Avg</font> ###")
        
        last_battery_level = get_battery_level()
        for day in charging_plan.keys():
            if not isinstance(day, int):
                continue
            
            overview.append("<details>\n")
            overview.append(f"<summary>🔎 Day {day} (discharge kwh: **{charging_plan[day]['discharge_kwh']:.1f}kWh**)</summary>\n")
            
            overview.append("| Hour | kwh<br>needed | kwh<br>not removed | price | cost | 🔋 |")
            overview.append("|:---:|:---:|:---:|:---:|:---:|:---:|")
            for hour in charging_plan[day]['hour_cost_prediction']["ema"].keys():
                ema_kwh = charging_plan[day]['hour_cost_prediction']["ema"][hour]['kwh']
                ema_kwh_not_removed = charging_plan[day]['hour_cost_prediction']["ema"][hour]['kwh_not_removed']
                ema_cost = charging_plan[day]['hour_cost_prediction']["ema"][hour]['cost']
                trend_kwh = charging_plan[day]['hour_cost_prediction']["trend"][hour]['kwh']
                trend_kwh_not_removed = charging_plan[day]['hour_cost_prediction']["trend"][hour]['kwh_not_removed']
                trend_cost = charging_plan[day]['hour_cost_prediction']["trend"][hour]['cost']
                avg_kwh = charging_plan[day]['hour_cost_prediction']["avg"][hour]['kwh']
                avg_kwh_not_removed = charging_plan[day]['hour_cost_prediction']["avg"][hour]['kwh_not_removed']
                avg_cost = charging_plan[day]['hour_cost_prediction']["avg"][hour]['cost']
                
                price = charging_plan[day]['hour_cost_prediction']["ema"][hour]['price']
                
                battery_level = round(sum(charging_plan[day]['battery_level_flow'][hour]), 1)
                
                kwh_string = f"<font color=red>{ema_kwh:.1f}</font> <br> <font color=yellow>{trend_kwh:.1f}</font> / <font color=green>{avg_kwh:.1f}</font>" if ema_kwh > 0.0 or trend_kwh > 0.0 or avg_kwh > 0.0 else f""
                kwh_not_removed_string = f"<font color=red>{ema_kwh_not_removed:.1f}</font> <br> <font color=yellow>{trend_kwh_not_removed:.1f} / <font color=green>{avg_kwh_not_removed:.1f}</font>" if ema_kwh_not_removed > 0.0 or trend_kwh_not_removed > 0.0 or avg_kwh_not_removed > 0.0 else f""
                price_string = f"{price:.2f}"
                cost_string = f"<font color=red>{ema_cost:.2f}</font> <br> <font color=yellow>{trend_cost:.2f}</font> / <font color=green>{avg_cost:.2f}</font>"
                
                battery_level_string = f"<font color=yellow>{battery_level:.1f}%</font>"
                if battery_level < last_battery_level:
                    battery_level_string = f"<font color=red>{battery_level:.1f}%</font> 📉"
                elif battery_level > last_battery_level:
                    battery_level_string = f"{emoji_parse({'charging': True})}<font color=green>{battery_level:.1f}%</font>"
                    
                last_battery_level = battery_level
                
                overview.append(f"| **{hour:02d}** | {kwh_string} | {kwh_not_removed_string} | {price_string} | {cost_string} | **{battery_level_string}** |")
                
            overview.append("</details>\n")
                    
        overview.append("</center>\n")
        overview.append("***")
    except Exception as e:
        _LOGGER.error(f"Hour cost prediction failed: {e} {type(e)}")
    
    if overview:
        overview.append("<center>\n")
        overview.append(f"##### {i18n.t('ui.cheap_grid_charge_hours.last_scheduled')} {getTime()} #####")
        overview.append("</center>\n")
        
        set_attr(f"sensor.{__name__}_overview.overview", "\n".join(overview))
    else:
        set_attr(f"sensor.{__name__}_overview.overview", "Ingen data")
    
    return chargeHours

def power_from_ignored(from_timestamp, to_timestamp):
    func_name = "power_from_ignored"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    
    def average_since_sum(entity_id):
        nonlocal func_name
        sub_func_name = f"average_since_sum"
        _LOGGER = globals()['_LOGGER'].getChild(f"{func_name}.{sub_func_name}")
        if entity_id == "" or entity_id is None: return 0.0

        try:
            avg = float(get_average_value(entity_id, from_timestamp, to_timestamp, convert_to="W", error_state=0.0))
            return avg
        except Exception:
            return 0.0

    total = 0.0
    try:
        if CONFIG['home']['entity_ids']['ignore_consumption_from_entity_ids']:
            for entity_id in CONFIG['home']['entity_ids']['ignore_consumption_from_entity_ids']:
                total += average_since_sum(entity_id)
    except Exception as e:
        _LOGGER.warning(f"Cant get ignore values from {from_timestamp} to {to_timestamp}: {e} {type(e)}")
    return round(total, 2)

def charge_from_powerwall(from_timestamp, to_timestamp) -> float:
    func_name = "charge_from_powerwall"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    
    powerwall_charging_consumption = 0.0
    
    try:
        powerwall_values = get_values(CONFIG['home']['entity_ids']['powerwall_watt_flow_entity_id'], from_timestamp, to_timestamp, float_type=True, convert_to="W", error_state=[0.0])

        if CONFIG['home']['invert_powerwall_watt_flow_entity_id']:
            powerwall_charging_consumption = abs(round(average(get_specific_values(powerwall_values, positive_only = True)), 0))
        else:
            powerwall_charging_consumption = abs(round(average(get_specific_values(powerwall_values, negative_only = True)), 0))
    except Exception as e:
        _LOGGER.warning(f"Cant get powerwall values from {from_timestamp} to {to_timestamp}: {e} {type(e)}")
    
    return powerwall_charging_consumption

def discharge_from_powerwall(from_timestamp, to_timestamp):
    func_name = "discharge_from_powerwall"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    
    powerwall_discharging_consumption = 0.0
    
    try:
        powerwall_values = get_values(CONFIG['home']['entity_ids']['powerwall_watt_flow_entity_id'], from_timestamp, to_timestamp, float_type=True, convert_to="W", error_state=[0.0])
        
        if CONFIG['home']['invert_powerwall_watt_flow_entity_id']:
            powerwall_discharging_consumption = abs(round(average(get_specific_values(powerwall_values, negative_only = True)), 0))
        else:
            powerwall_discharging_consumption = abs(round(average(get_specific_values(powerwall_values, positive_only = True)), 0))
    except Exception as e:
        _LOGGER.warning(f"Cant get powerwall values from {from_timestamp} to {to_timestamp}: {e} {type(e)}")
        
    return powerwall_discharging_consumption

def power_values(from_timestamp = None, to_timestamp = None, period = None):
    func_name = "power_values"
    func_prefix = f"{func_name}_"
    func_id = random.randint(100000, 999999)
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    global TASKS
    
    task_names = {
        "power_consumption": f"{func_prefix}power_consumption_{func_id}",
        "ignored_consumption": f"{func_prefix}ignored_consumption_{func_id}",
        "powerwall_charging_consumption": f"{func_prefix}powerwall_charging_consumption_{func_id}",
        "powerwall_discharging_consumption": f"{func_prefix}powerwall_discharging_consumption_{func_id}",
        "solar_production": f"{func_prefix}solar_production_{func_id}"
    }
    
    power_consumption = 0.0
    ignored_consumption = 0.0
    powerwall_charging_consumption = 0.0
    powerwall_discharging_consumption = 0.0
    solar_production = 0.0
    power_consumption_without_ignored = 0.0
    power_consumption_without_all_exclusion = 0.0
    
    try:
        if period is not None:
            now = getTime()
            to_timestamp = now
            from_timestamp = now - datetime.timedelta(minutes=period)
        
        TASKS[task_names["power_consumption"]] = task.create(get_average_value, CONFIG['home']['entity_ids']['power_consumption_entity_id'], from_timestamp, to_timestamp, convert_to="W", error_state=0.0)
        TASKS[task_names["ignored_consumption"]] = task.create(power_from_ignored, from_timestamp, to_timestamp)
        TASKS[task_names["powerwall_charging_consumption"]] = task.create(charge_from_powerwall, from_timestamp, to_timestamp)
        TASKS[task_names["powerwall_discharging_consumption"]] = task.create(discharge_from_powerwall, from_timestamp, to_timestamp)
        TASKS[task_names["solar_production"]] = task.create(get_average_value, CONFIG['solar']['entity_ids']['production_entity_id'], from_timestamp, to_timestamp, convert_to="W", error_state=0.0)
        
        done, pending = task.wait({TASKS[task_names["power_consumption"]], TASKS[task_names["ignored_consumption"]],
                                TASKS[task_names["powerwall_charging_consumption"]], TASKS[task_names["powerwall_discharging_consumption"]],
                                TASKS[task_names["solar_production"]]})
        
        power_consumption = abs(round(float(TASKS[task_names["power_consumption"]].result()), 2))
        ignored_consumption = abs(TASKS[task_names["ignored_consumption"]].result())
        powerwall_charging_consumption = TASKS[task_names["powerwall_charging_consumption"]].result()
        powerwall_discharging_consumption = TASKS[task_names["powerwall_discharging_consumption"]].result()
        solar_production = abs(round(float(TASKS[task_names["solar_production"]].result()), 2))
                    
        if not CONFIG['home']['power_consumption_entity_id_include_powerwall_discharging']:
            power_consumption += powerwall_discharging_consumption
                
        if CONFIG['home']['power_consumption_entity_id_include_powerwall_charging']:
            power_consumption -= powerwall_charging_consumption
            
        power_consumption_without_ignored = round(power_consumption - ignored_consumption, 2)
        power_consumption_without_all_exclusion = max(round(power_consumption_without_ignored - powerwall_charging_consumption, 2), 0.0)
    except (asyncio.CancelledError, asyncio.TimeoutError, KeyError) as e:
        _LOGGER.warning(f"Cancelled/timeout getting power values from {from_timestamp} to {to_timestamp} or period {period}: {e} {type(e)}")
        return
    except Exception as e:
        _LOGGER.error(f"Failed to get power values from {from_timestamp} to {to_timestamp} or period {period}: {e} {type(e)}")
        my_persistent_notification(
            f"Failed to get power values from {from_timestamp} to {to_timestamp} or period {period}: {e} {type(e)}",
            title=f"{TITLE} error",
            persistent_notification_id=f"{__name__}_{func_name}"
        )
    finally:
        task_cancel(task_names.values(), task_remove=True, timeout=3.0, wait_period=0.2)
            
    return {
        "power_consumption": power_consumption,
        "ignored_consumption": ignored_consumption,
        "powerwall_charging_consumption": powerwall_charging_consumption,
        "powerwall_discharging_consumption": powerwall_discharging_consumption,
        "solar_production": solar_production,
        "power_consumption_without_ignored": power_consumption_without_ignored,
        "power_consumption_without_all_exclusion": power_consumption_without_all_exclusion
    }

def local_energy_available(period=None, from_timestamp=None, to_timestamp=None, solar_only=False, without_all_exclusion=False, include_local_energy_distribution=False):
    func_name = "local_energy_available"
    _LOGGER = globals()["_LOGGER"].getChild(func_name)
    global TASKS
    
    watts_available_from_local_energy = 0.0
    watts_available_from_local_energy_solar_only = 0.0
    
    try:
        if period is not None:
            period = max(period, CONFIG["cron_interval"])
            now = getTime()
            to_timestamp = now
            from_timestamp = now - datetime.timedelta(minutes=period)

        values = power_values(from_timestamp = from_timestamp, to_timestamp = to_timestamp)
        
        ignored_consumption = values["ignored_consumption"]
        power_consumption = values["power_consumption"]
        power_consumption_without_ignored = values["power_consumption_without_ignored"]
        power_consumption_without_all_exclusion = values["power_consumption_without_all_exclusion"]
        powerwall_charging_consumption = values["powerwall_charging_consumption"]
        powerwall_discharging_consumption = values["powerwall_discharging_consumption"]
        solar_production = values["solar_production"]
        total_local_energy = solar_production + powerwall_discharging_consumption
        
        house_power_consumption = power_consumption_without_all_exclusion if without_all_exclusion else power_consumption_without_ignored
        
        watts_available_from_local_energy = max(total_local_energy - house_power_consumption, 0.0)
        watts_available_from_local_energy_solar_only = max(solar_production - house_power_consumption, 0.0)
        powerwall_discharging_available = max(watts_available_from_local_energy - watts_available_from_local_energy_solar_only, 0.0)
        
        watts_available_from_local_energy = round(min(watts_available_from_local_energy, CONFIG["solar"]["inverter_discharging_power_limit"]), 2)

        if include_local_energy_distribution:
            solar_watts_of_local_energy = 0.0
            _LOGGER.debug(f"values: {values} watts_available_from_local_energy: {watts_available_from_local_energy} solar_production: {solar_production} total_local_energy: {total_local_energy} powerwall_discharging_available: {powerwall_discharging_available}")
            watts_from_local_energy = watts_available_from_local_energy
            
            if watts_from_local_energy > 0.0:
                solar_watts_of_local_energy = (solar_production / total_local_energy) * watts_from_local_energy if total_local_energy > 0.0 else 0.0
            
            return watts_available_from_local_energy_solar_only if solar_only else watts_available_from_local_energy, watts_from_local_energy, solar_watts_of_local_energy
    except Exception as e:
        _LOGGER.error(f"Error calculating local energy available: {e} {type(e)}")
        _LOGGER.error(f"parameters: period: {period} from_timestamp: {from_timestamp} to_timestamp: {to_timestamp} solar_only: {solar_only} without_all_exclusion: {without_all_exclusion} include_local_energy_distribution: {include_local_energy_distribution}")
        my_persistent_notification(
            f"Error calculating local energy available: {e} {type(e)}",
            title=f"{TITLE} error",
            persistent_notification_id=f"{__name__}_{func_name}"
        )
        
        if include_local_energy_distribution:
            return watts_available_from_local_energy, 0.0, 0.0
    
    return watts_available_from_local_energy_solar_only if solar_only else watts_available_from_local_energy

def inverter_available(error = ""):
    func_name = "inverter_available"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    
    if not is_solar_configured(): return False
    
    if not is_entity_configured(CONFIG['home']['entity_ids']['power_consumption_entity_id']) or is_entity_available(CONFIG['home']['entity_ids']['power_consumption_entity_id']):
        return True
    
    _LOGGER.error(f"Inverter not available ({CONFIG['home']['entity_ids']['power_consumption_entity_id']} = {get_state(CONFIG['home']['entity_ids']['power_consumption_entity_id'])}): {error}")
    return False

def get_forecast_dict():
    func_name = "get_forecast_dict"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    
    hourly_forecast= {}
    daily_forecast= {}
    return_dict = {
        "hourly": {},
        "daily": {}
    }
    
    try:
        if not service.has_service('weather', 'get_forecasts'):
            raise Exception("Forecast service not available: weather.get_forecasts")
        
        if CONFIG['forecast']['entity_ids']['hourly_service_entity_id']:
            hourly_forecast = service.call("weather", "get_forecasts", blocking=True,
                                    entity_id=CONFIG['forecast']['entity_ids']['hourly_service_entity_id'],
                                    type="hourly")
            
            if CONFIG['forecast']['entity_ids']['hourly_service_entity_id'] in hourly_forecast:
                return_dict["hourly"] = hourly_forecast[CONFIG['forecast']['entity_ids']['hourly_service_entity_id']]["forecast"]
            else:
                raise Exception(f"{CONFIG['forecast']['entity_ids']['hourly_service_entity_id']} not in return dict")
    except Exception as e:
        _LOGGER.error(f"Cant get hourly forecast dict: {e} {type(e)}")
        
    try:
        if not service.has_service('weather', 'get_forecasts'):
            raise Exception("Forecast service not available: weather.get_forecasts")
        
        if CONFIG['forecast']['entity_ids']['daily_service_entity_id']:
            daily_forecast = service.call("weather", "get_forecasts", blocking=True,
                                    entity_id=CONFIG['forecast']['entity_ids']['daily_service_entity_id'],
                                    type="daily")
            
            if CONFIG['forecast']['entity_ids']['daily_service_entity_id'] in daily_forecast:
                return_dict["daily"] = daily_forecast[CONFIG['forecast']['entity_ids']['daily_service_entity_id']]["forecast"]
            else:
                raise Exception(f"{CONFIG['forecast']['entity_ids']['daily_service_entity_id']} not in return dict")
    except Exception as e:
        _LOGGER.error(f"Cant get daily forecast dict: {e} {type(e)}")
        
    return return_dict

def get_forecast(forecast_dict = None, date = None):
    func_name = "get_forecast"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    
    if date is None:
        date = getTimeStartOfDay()
    
    forecast = None
    try:
        if "hourly" in forecast_dict:
            for data in forecast_dict["hourly"]:
                if data is None:
                    continue
                
                date = reset_time_to_hour(date)
                forecastDate = reset_time_to_hour(toDateTime(data['datetime']))
                if date == forecastDate:
                    forecast = data
                    break
                
        if forecast is None and "daily" in forecast_dict:
            for data in forecast_dict["daily"]:
                if data is None:
                    continue
                
                date = date.replace(hour=0, minute=0, second=0, tzinfo=None)
                forecastDate = toDateTime(data['datetime']).replace(hour=0, minute=0, second=0, tzinfo=None)
                if date == forecastDate:
                    forecast = data
                    break
    except Exception as e:
        _LOGGER.error(f"Cant get forecast for date {date} {e} {type(e)}")
        
    _LOGGER.debug(f"{date}: {forecast}")
    return forecast
    
def forecast_score(data):
    func_name = "forecast_score"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    
    try:
        if "cloud_coverage" not in data or "uv_index" not in data or "temperature" not in data:
            if "condition" in data and "datetime" in data:
                _LOGGER.debug(f"{data['datetime']}: Missing cloud_coverage, uv_index or temperature in data, using condition: {data['condition']} = {WEATHER_CONDITION_DICT[data['condition']]}")
                return WEATHER_CONDITION_DICT[data["condition"]]
            
            raise Exception("Missing required keys in data")
            
        normalized_cloud_coverage = (100 - data["cloud_coverage"]) / 100
        
        uv_index = data["uv_index"]
        max_uv_index = 11
        normalized_uv_index = min(uv_index / max_uv_index, 1)
        
        temperature = data["temperature"]
        max_temperature = 20
        if temperature <= max_temperature:
            normalized_temperature = temperature / max_temperature
        else:
            normalized_temperature = max_temperature / temperature

        cloud_weight = 0.5
        uv_weight = 0.3
        temperature_weight = 0.2

        if data["cloud_coverage"] >= 90:
            normalized_cloud_coverage *= 0.2
            normalized_uv_index *= 0.1
            normalized_temperature *= 0.5

        score = ((
            normalized_cloud_coverage * cloud_weight +
            normalized_uv_index * uv_weight +
            normalized_temperature * temperature_weight
        ) * 100)
    except Exception as e:
        _LOGGER.error(f"Cant calculate forecast score, using 50.0: {e}\n{data}")
        score = 50.0

    return score

def db_cloud_coverage_to_score(database):
    new_database = {}
    for hour in database:
        new_database[hour] = {}
        for cloud_coverage, data in database[hour].items():
            score = 100 - cloud_coverage
            new_database[hour][score] = data
    return new_database

def transform_database(database, step_size=20):
    new_database = {}
    for hour, cloud_data in database.items():
        new_database[hour] = {}

        for cloud_coverage, entries in cloud_data.items():
            score_group = (cloud_coverage // step_size) * step_size
            upper_group = score_group + step_size

            if score_group not in new_database[hour]:
                new_database[hour][score_group] = []
            if upper_group not in new_database[hour] and upper_group <= 100:
                new_database[hour][upper_group] = []

            sorted_entries = sorted(entries, key=lambda x: x[1])

            split_index = len(sorted_entries) // 2

            new_database[hour][score_group].extend(sorted_entries[:split_index])

            if upper_group <= 100:
                new_database[hour][upper_group].extend(sorted_entries[split_index:])

    return new_database

def load_power_values_db():
    func_name = "load_power_values_db"
    func_prefix = f"{func_name}_"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    global POWER_VALUES_DB
    
    if not is_solar_configured() or not CONFIG['solar']['entity_ids']['forecast_entity_id']: return
    
    version = 1.0
    
    try:
        filename = f"{__name__}_power_values_db"
        TASKS[f'{func_prefix}load_yaml'] = task.create(load_yaml, filename)
        done, pending = task.wait({TASKS[f'{func_prefix}load_yaml']})
        database = TASKS[f'{func_prefix}load_yaml'].result()
        
        if "version" in database:
            version = float(database["version"])
            del database["version"]
            
        POWER_VALUES_DB = deepcopy(database)
        
        if not POWER_VALUES_DB:
            TASKS[f'{func_prefix}create_yaml'] = task.create(create_yaml, filename, db=POWER_VALUES_DB)
            done, pending = task.wait({TASKS[f'{func_prefix}create_yaml']})
    except (asyncio.CancelledError, asyncio.TimeoutError, KeyError) as e:
        _LOGGER.warning(f"Cancelled/timeout loading {__name__}_power_values_db: {e} {type(e)}")
        return
    except Exception as e:
        error_message = f"Cant load {__name__}_power_values_db: {e} {type(e)}"
        _LOGGER.error(error_message)
        save_error_to_file(error_message, caller_function_name = f"{func_name}()")
        my_persistent_notification(
            f"Failed to load {__name__}_power_values_db",
            title=f"{TITLE} error",
            persistent_notification_id=f"{__name__}_{func_name}"
        )
    finally:
        task_cancel(func_prefix, task_remove=True, startswith=True)
    
    if POWER_VALUES_DB == {} or not POWER_VALUES_DB:
        POWER_VALUES_DB = {}

    for h in range(24):
        if h not in POWER_VALUES_DB:
            POWER_VALUES_DB[h] = {}
        else: #Remove BattMind keys if exists
            dict_key_list = ("power_consumption", "ignored_consumption", "powerwall_charging_consumption",
                             "powerwall_discharging_consumption", "solar_production", "power_consumption_without_ignored",
                             "power_consumption_without_all_exclusion")
            for key in list(POWER_VALUES_DB[h].keys()):
                if key not in dict_key_list:
                    POWER_VALUES_DB[h].pop(key, None)
            
    save_power_values_db()
    
def save_power_values_db():
    func_name = "save_power_values_db"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    global POWER_VALUES_DB
    
    if not is_solar_configured() or not CONFIG['solar']['entity_ids']['forecast_entity_id']: return
    
    if len(POWER_VALUES_DB) > 0:
        db_to_file = deepcopy(POWER_VALUES_DB)
        db_to_file["version"] = POWER_VALUES_DB_VERSION
        save_changes(f"{__name__}_power_values_db", db_to_file)

def power_values_to_db(data):
    func_name = "power_values_to_db"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    global POWER_VALUES_DB
    
    if not is_solar_configured() or not CONFIG['solar']['entity_ids']['forecast_entity_id']: return
    
    if not inverter_available(f"{func_name}({data})"):
        return
    
    if len(POWER_VALUES_DB) == 0:
        load_power_values_db()
        
    hour = getHour()
    
    for key, value in data.items():
        if key not in POWER_VALUES_DB[hour]:
            POWER_VALUES_DB[hour][key] = []
            
        POWER_VALUES_DB[hour][key].insert(0, [getTime(), value])
        _LOGGER.debug(f"inserting {value} POWER_VALUES_DB[{hour}] = {POWER_VALUES_DB[hour]}")
        
        POWER_VALUES_DB[hour][key] = POWER_VALUES_DB[hour][key][:CONFIG['database']['power_values_db_data_to_save']]
        _LOGGER.debug(f"removing values over {CONFIG['database']['power_values_db_data_to_save']} POWER_VALUES_DB[{hour}] = {POWER_VALUES_DB[hour][key]}")
    
    save_power_values_db()
    
def load_solar_available_db():
    func_name = "load_solar_available_db"
    func_prefix = f"{func_name}_"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    global SOLAR_PRODUCTION_AVAILABLE_DB
    
    if not is_solar_configured(): return
    
    version = 1.0
    
    try:
        filename = f"{__name__}_solar_production_available_db"
        TASKS[f'{func_prefix}load_yaml'] = task.create(load_yaml, filename)
        done, pending = task.wait({TASKS[f'{func_prefix}load_yaml']})
        database = TASKS[f'{func_prefix}load_yaml'].result()
        
        if "version" in database:
            version = float(database["version"])
            del database["version"]
            
        SOLAR_PRODUCTION_AVAILABLE_DB = deepcopy(database)
        
        if not SOLAR_PRODUCTION_AVAILABLE_DB:
            TASKS[f'{func_prefix}create_yaml'] = task.create(create_yaml, filename, db=SOLAR_PRODUCTION_AVAILABLE_DB)
            done, pending = task.wait({TASKS[f'{func_prefix}create_yaml']})
    except Exception as e:
        error_message = f"Cant load {__name__}_solar_production_available_db: {e} {type(e)}"
        _LOGGER.error(error_message)
        save_error_to_file(error_message, caller_function_name = f"{func_name}()")
        my_persistent_notification(
            f"Failed to load {__name__}_solar_production_available_db",
            title=f"{TITLE} error",
            persistent_notification_id=f"{__name__}_{func_name}"
        )
    except (asyncio.CancelledError, asyncio.TimeoutError, KeyError) as e:
        _LOGGER.warning(f"Cancelled/timeout loading {__name__}_solar_production_available_db: {e} {type(e)}")
        return
    finally:
        task_cancel(func_prefix, task_remove=True, startswith=True)
    
    if SOLAR_PRODUCTION_AVAILABLE_DB == {} or not SOLAR_PRODUCTION_AVAILABLE_DB:
        SOLAR_PRODUCTION_AVAILABLE_DB = {}
    
    for h in range(24):
        if h not in SOLAR_PRODUCTION_AVAILABLE_DB:
            SOLAR_PRODUCTION_AVAILABLE_DB[h] = {}
        for value in weather_values():
            SOLAR_PRODUCTION_AVAILABLE_DB[h].setdefault(value, [])
    
    if version <= 1.0:
        _LOGGER.info(f"Transforming database from version {version} to {SOLAR_PRODUCTION_AVAILABLE_DB_VERSION}")
        SOLAR_PRODUCTION_AVAILABLE_DB = db_cloud_coverage_to_score(SOLAR_PRODUCTION_AVAILABLE_DB)
        SOLAR_PRODUCTION_AVAILABLE_DB = transform_database(SOLAR_PRODUCTION_AVAILABLE_DB)
        
    save_solar_available_db()
    
def save_solar_available_db():
    func_name = "save_solar_available_db"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    global SOLAR_PRODUCTION_AVAILABLE_DB
    
    if not is_solar_configured(): return
    
    if len(SOLAR_PRODUCTION_AVAILABLE_DB) > 0:
        db_to_file = deepcopy(SOLAR_PRODUCTION_AVAILABLE_DB)
        db_to_file["version"] = SOLAR_PRODUCTION_AVAILABLE_DB_VERSION
        save_changes(f"{__name__}_solar_production_available_db", db_to_file)
        
def solar_available_append_to_db(power):
    func_name = "solar_available_append_to_db"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    global SOLAR_PRODUCTION_AVAILABLE_DB
    
    if not is_solar_configured(): return
    
    if not inverter_available(f"solar_available_append_to_db({power})"):
        return
    
    if len(SOLAR_PRODUCTION_AVAILABLE_DB) == 0:
        load_solar_available_db()
    
    hour = getHour()
    cloudiness = None
    cloudiness_score = None
    
    forecast_dict = get_forecast_dict()
    forecast = get_forecast(forecast_dict)
    cloudiness = forecast_score(forecast)
    
    
    if cloudiness is not None:
        cloudiness_score = get_closest_key(cloudiness, SOLAR_PRODUCTION_AVAILABLE_DB[hour], return_key=True)
    else:
        try:
            try:
                cloudiness_score = WEATHER_CONDITION_DICT[get_state(CONFIG['forecast']['entity_ids']['hourly_service_entity_id'])]
            except:
                cloudiness_score = WEATHER_CONDITION_DICT[get_state(CONFIG['forecast']['entity_ids']['daily_service_entity_id'])]
        except Exception as e:
            _LOGGER.error(f"Cant get states from hourly {CONFIG['forecast']['entity_ids']['daily_service_entity_id']} or daily {CONFIG['forecast']['entity_ids']['daily_service_entity_id']}: {e} {type(e)}")
            return
    
    SOLAR_PRODUCTION_AVAILABLE_DB[hour][cloudiness_score].insert(0, [getTime(), power])
    _LOGGER.debug(f"inserting {power} SOLAR_PRODUCTION_AVAILABLE_DB[{hour}][{cloudiness_score}] = {SOLAR_PRODUCTION_AVAILABLE_DB[hour][cloudiness_score]}")
    
    SOLAR_PRODUCTION_AVAILABLE_DB[hour][cloudiness_score] = SOLAR_PRODUCTION_AVAILABLE_DB[hour][cloudiness_score][:CONFIG['database']['solar_available_db_data_to_save']]
    _LOGGER.debug(f"removing values over {CONFIG['database']['solar_available_db_data_to_save']} SOLAR_PRODUCTION_AVAILABLE_DB[{hour}][{cloudiness_score}] = {SOLAR_PRODUCTION_AVAILABLE_DB[hour][cloudiness_score]}")
    
    save_solar_available_db()

def get_solar_kwh_forecast():
    func_name = "get_solar_kwh_forecast"
    func_prefix = f"{func_name}_"
    func_id = random.randint(100000, 999999)
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    global POWER_VALUES_DB
    
    if "forecast_entity_id" in CONFIG['solar']['entity_ids'] and not CONFIG['solar']['entity_ids']['forecast_entity_id']:
        return {}
    
    def forecast_task(base_entity_id, ending):
        _LOGGER = globals()['_LOGGER'].getChild(f"{func_name}.forecast_task")
        nonlocal forecast
        
        try:
            entity_id = f"{base_entity_id}_{ending}"
            site_attr = get_attr(entity_id, "detailedHourly", error_state={})
            
            for data in site_attr:
                date = data['period_start'].replace(tzinfo=None)
                
                if date not in hour_prices:
                    continue
                
                watt = round(data['pv_estimate'] * 1000.0, 0)
                                        
                power_consumption_without_all_exclusion = calculate_ema(reverse_list(get_list_values(POWER_VALUES_DB[date.hour].get("power_consumption_without_all_exclusion", [0.0]))))
                available = max(watt - power_consumption_without_all_exclusion, 0.0)
                available_kwh = round(available / 1000.0, 3)
                
                day_of_week = getDayOfWeek(date)
                tariff_dict = get_tariffs(date.hour, day_of_week)
                transmissions_nettarif = tariff_dict["transmissions_nettarif"]
                systemtarif = tariff_dict["systemtarif"]
                tariff_sum = tariff_dict["tariff_sum"]
                
                price = hour_prices[date]
                raw_price = price - tariff_sum
                
                sell_tariffs = sum((solar_production_seller_cut, energinets_network_tariff, energinets_balance_tariff, transmissions_nettarif, systemtarif))
                sell_price = raw_price - sell_tariffs
                
                forecast[date] = (available_kwh, sell_price)
        except Exception as e:
            _LOGGER.error(f"Error in forecast_task for {base_entity_id}_{ending}: {e} {type(e)}")
    
    def strip_forecast_suffix(entity_id: str, endings: list) -> str:
        parts = entity_id.split("_")

        if parts[-1] in endings:
            return "_".join(parts[:-1])

        if len(parts) >= 2 and parts[-2] == "day" and parts[-1].isdigit():
            return "_".join(parts[:-2])
    
    forecast = {}
    
    hour_prices = get_hour_prices()
                        
    energinets_network_tariff = SOLAR_SELL_TARIFF["energinets_network_tariff"]
    energinets_balance_tariff = SOLAR_SELL_TARIFF["energinets_balance_tariff"]
    solar_production_seller_cut = SOLAR_SELL_TARIFF["solar_production_seller_cut"]
    
    integration = get_integration(CONFIG['solar']['entity_ids']['forecast_entity_id'])
    
    if integration == "solcast_solar":
        endings = ["today", "tomorrow", "day_3", "day_4", "day_5", "day_6", "day_7"]
        base_entity_id = strip_forecast_suffix(CONFIG['solar']['entity_ids']['forecast_entity_id'], endings)
        
        task_set = set()
        try:
            for ending in endings:
                task_name = f"{func_prefix}{ending}_{func_id}"
                
                TASKS[task_name] = task.create(forecast_task, base_entity_id=base_entity_id, ending=ending)
                TASKS[task_name].set_name(task_name)
                
                task_set.add(TASKS[task_name])
        
            done, pending = task.wait(task_set)
        except Exception as e:
            _LOGGER.error(f"Error: {e} {type(e)}")
        except (asyncio.CancelledError, asyncio.TimeoutError, KeyError) as e:
            _LOGGER.warning(f"Cancelled/timeout getting solar kwh forecast: {e} {type(e)}")
            return
        finally:
            task_cancel(task_set, task_remove=True)
            
    return forecast

def local_energy_prediction(powerwall_charging_timestamps = False):
    func_name = "local_energy_prediction"
    func_prefix = f"{func_name}_"
    func_id = random.randint(100000, 999999)
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    global TASKS, SOLAR_PRODUCTION_AVAILABLE_DB, LOCAL_ENERGY_PREDICTION_DB
    
    def get_database_kwh(cloudiness: int | float, date: datetime.datetime) -> list:
        nonlocal func_name, sell_price
        sub_func_name = "get_database_kwh"
        _LOGGER = globals()['_LOGGER'].getChild(f"{func_name}.{sub_func_name}")
        
        hour = getHour(date)
        day_of_week = getDayOfWeek(date)
        
        try:
            power_list = reverse_list(get_list_values(get_closest_key(cloudiness, SOLAR_PRODUCTION_AVAILABLE_DB[hour])))
            power_one_down_list = []
            power_one_up_list = []
            if type(power_list) == list:
                if len(power_list) <= 6 or calculate_ema(power_list) <= 1000.0:
                    if cloudiness >= 20:
                        power_one_down_list = reverse_list(get_list_values(get_closest_key(cloudiness - 20, SOLAR_PRODUCTION_AVAILABLE_DB[hour])))
                    if cloudiness <= 80:
                        power_one_up_list = reverse_list(get_list_values(get_closest_key(cloudiness + 20, SOLAR_PRODUCTION_AVAILABLE_DB[hour])))
                           
                _LOGGER.debug(f"{hour} cloudiness:{cloudiness}% power_list: average({average(power_list)})/ema({calculate_ema(power_list)})={power_list}\npower_one_down: average({average(power_one_down_list)})/ema({calculate_ema(power_one_down_list)})={power_one_down_list}\npower_one_up: average({average(power_one_up_list)})/ema({calculate_ema(power_one_up_list)})={power_one_up_list}")
                
                power_list = [calculate_ema(power_list)] if power_list else []
                power_one_down_list = [calculate_ema(power_one_down_list)] if power_one_down_list else []
                power_one_up_list = [calculate_ema(power_one_up_list)] if power_one_up_list else []
                
                avg_power = max(average(power_list + power_list + power_list + power_one_down_list + power_one_up_list), 0.0)
                avg_kwh = avg_power / 1000
                
                avg_sell_price = sell_price
                
                if avg_sell_price == -1.0:
                    avg_sell_price = average(KWH_AVG_PRICES_DB['history_sell'][hour][day_of_week])
                
                return [avg_kwh, avg_sell_price]
        except Exception as e:
            _LOGGER.warning(f"Cant get cloudiness: {cloudiness}, hour: {hour}, day_of_week: {day_of_week}. {e} {type(e)}")
        return [0.0, 0.0]

    def process_forecast(loop_datetime, is_away, current_hour_factor, cloudiness, solar_forecast_from_integration,
                        total_db, total_db_sell, total_forecast, total_forecast_sell,
                        total_avg, total_avg_sell):
        nonlocal func_name
        sub_func_name = "process_forecast"
        _LOGGER = globals()['_LOGGER'].getChild(f"{func_name}.{sub_func_name}")
        nonlocal solar_prediction_timestamps_dict, powerwall_charging_timestamps_dict
        
        loop_kwh = []
        loop_sell = []

        # Cloudiness forecast
        if cloudiness is not None:
            power_factor = 0.5 if loop_datetime in solar_forecast_from_integration else 1.0
            power_factor *= current_hour_factor
            power_cost = get_database_kwh(cloudiness, loop_datetime)
            loop_kwh.append(power_cost[0] * power_factor)
            loop_sell.append(power_cost[1] * power_factor)
            total_db.append(round(power_cost[0], 3))
            total_db_sell.append(round(power_cost[1], 3))

        # Solar forecast
        if loop_datetime in solar_forecast_from_integration:
            power_factor = 1.5 if cloudiness is not None else 1.0
            power_factor *= current_hour_factor
            loop_kwh.append(solar_forecast_from_integration[loop_datetime][0] * power_factor)
            loop_sell.append(solar_forecast_from_integration[loop_datetime][1] * power_factor)
            total_forecast.append(round(solar_forecast_from_integration[loop_datetime][0], 3))
            total_forecast_sell.append(round(solar_forecast_from_integration[loop_datetime][1], 3))
            
        # Average forecast
        if loop_kwh:
            loop_kwh = round(average(loop_kwh), 3)
            loop_sell = round(average(loop_sell), 3)
            total_avg.append(loop_kwh)
            total_avg_sell.append(loop_sell)
                
    def day_prediction_task(day, today, sunrise, sunset, stop_prediction_before, forecast_dict, solar_forecast_from_integration, using_grid_price):
        nonlocal func_name
        sub_func_name = "day_prediction_task"
        _LOGGER = globals()['_LOGGER'].getChild(f"{func_name}.{sub_func_name}")
        global LOCAL_ENERGY_PREDICTION_DB
        nonlocal output, output_sell, solar_prediction_timestamps_dict, powerwall_charging_timestamps_dict
        
        date = today + datetime.timedelta(days=day)
        output[date] = 0.0
        output_sell[date] = 0.0
        
        dayName = getDayOfWeekText(getTimePlusDays(day))
        total_database = []
        total_database_sell = []
        total_forecast = []
        total_forecast_sell = []
        total = []
        total_sell = []
        
            
        current_hour = getHour()
        
        if forecast_dict:
            for hour in range(24):
                loop_datetime = date.replace(hour = hour)
                
                forecast = get_forecast(forecast_dict, loop_datetime)
                cloudiness = forecast_score(forecast) if forecast is not None else None
                
                current_hour_factor = abs((getMinute() / 60.0) - 1) if day == 0 and hour == current_hour else 1.0
                is_away = False
                
                process_forecast(
                    loop_datetime, is_away, current_hour_factor,
                    cloudiness, solar_forecast_from_integration,
                    total_database, total_database_sell,
                    total_forecast, total_forecast_sell,
                    total, total_sell
                )
                
            if not powerwall_charging_timestamps:
                if day not in LOCAL_ENERGY_PREDICTION_DB["solar_prediction"]:
                    LOCAL_ENERGY_PREDICTION_DB["solar_prediction"][day] = {}
                    
                LOCAL_ENERGY_PREDICTION_DB["solar_prediction"][day] = {
                    "total_database": total_database,
                    "total_database_sell": total_database_sell,
                    "total_forecast": total_forecast,
                    "total_forecast_sell": total_forecast_sell,
                    "total": total,
                    "total_sell": total_sell
                }
            
            total = round(sum(total), 3)
            total_sell = round(average(total_sell), 3)
            
            if day == 0:
                output['today'] = total
                output_sell['today'] = total_sell
        else:
            if day == 0:
                output['today'] = sum(total)
                output_sell['today'] = average(total_sell)
            total = None
            total_sell = None

        output[date] = total
        output_sell[date] = total_sell

    sell_price = float(get_state(f"input_number.{__name__}_solar_sell_fixed_price", float_type=True, error_state=CONFIG['solar']['production_price']))
    stop_prediction_before = 3
    days = 7
    today = getTimeStartOfDay()
    output = {
        "avg": 0,
        "today": 0
    }
    output_sell = {
        "avg": 0,
        "today": 0
    }
    
    if not is_solar_configured() or not inverter_available(f"Inverter not available)"):
        if powerwall_charging_timestamps:
            return []
        return output, output_sell
    
    try:
        now = getTime()
        location = sun.get_astral_location(hass)
        sunrise = location[0].sunrise(now).replace(tzinfo=None).hour
        sunset = location[0].sunset(now).replace(tzinfo=None).hour
    except Exception as e:
        _LOGGER.error(f"Cant get sunrise/sunset: {e} {type(e)}")
        return output, output_sell
    
    using_grid_price = True if float(get_state(f"input_number.{__name__}_solar_sell_fixed_price", float_type=True, error_state=CONFIG['solar']['production_price'])) == -1.0 else False
    
    solar_prediction_timestamps_dict = {}
    powerwall_charging_timestamps_dict = {}
    
    task_set = set()
    day_prediction_task_set = set()
    
    forecast_dict = {}
    solar_forecast_from_integration = {}
    
    try:
        TASKS[f"{func_prefix}forecast_dict_{func_id}"] = task.create(get_forecast_dict)
        TASKS[f"{func_prefix}forecast_dict_{func_id}"].set_name(f"{func_prefix}forecast_dict_{func_id}")
        
        TASKS[f"{func_prefix}solar_forecast_from_integration_{func_id}"] = task.create(get_solar_kwh_forecast)
        TASKS[f"{func_prefix}solar_forecast_from_integration_{func_id}"].set_name(f"{func_prefix}solar_forecast_from_integration_{func_id}")
        
        task_set.add(TASKS[f"{func_prefix}forecast_dict_{func_id}"])
        task_set.add(TASKS[f"{func_prefix}solar_forecast_from_integration_{func_id}"])
        
        done, pending = task.wait(task_set)
        
        forecast_dict = TASKS[f"{func_prefix}forecast_dict_{func_id}"].result()
        solar_forecast_from_integration = TASKS[f"{func_prefix}solar_forecast_from_integration_{func_id}"].result()
        
        for day in range(days + 1):
            task_name = f"{func_prefix}day_prediction_task_{day}_{func_id}"
            
            TASKS[task_name] = task.create(day_prediction_task, day, today, sunrise, sunset, stop_prediction_before, forecast_dict, solar_forecast_from_integration, using_grid_price)
            TASKS[task_name].set_name(task_name)
            
            day_prediction_task_set.add(TASKS[task_name])
        
        done, pending = task.wait(day_prediction_task_set)
    except (asyncio.CancelledError, asyncio.TimeoutError, KeyError) as e:
        _LOGGER.warning(f"Cancelled/timeout during day prediction tasks: {e} {type(e)}")
        return
    except Exception as e:
        _LOGGER.error(f"Error during day prediction tasks: {e} {type(e)}")
        my_persistent_notification(
            f"Failed to run local energy prediction tasks",
            title=f"{TITLE} error",
            persistent_notification_id=f"{__name__}_{func_name}_tasks"
        )
    finally:
        task_cancel(task_set, task_remove=True)
        task_cancel(day_prediction_task_set, task_remove=True)
    
    if solar_prediction_timestamps_dict:
        LOCAL_ENERGY_PREDICTION_DB["solar_prediction_timestamps"] = solar_prediction_timestamps_dict
    
    if powerwall_charging_timestamps:
        LOCAL_ENERGY_PREDICTION_DB["powerwall_charging_timestamps"] = powerwall_charging_timestamps_dict
        return powerwall_charging_timestamps_dict.keys()
    
    avg = []
    avg_sell = []
    for key in output.keys():
        if key not in ("avg", "today") and output[key] is not None:
                avg.append(output[key])
    output['avg'] = average(avg)

    for key in output_sell.keys():
        if key not in ("avg", "today") and output_sell[key] is not None and output[key] != 0.0:
                avg_sell.append(output_sell[key])
    output_sell['avg'] = average(avg_sell)
    
    for date in output.keys():
        if output[date] is None:
            output[date] = output['avg']
            
    for date in output_sell.keys():
        if output_sell[date] is None:
            output_sell[date] = output_sell['avg']
            
    return output, output_sell

def current_hour_in_charge_hours():
    current_hour = reset_time_to_hour()
    for timestamp in CHARGE_HOURS:
        if isinstance(timestamp, datetime.datetime):
            if reset_time_to_hour(timestamp) == current_hour:
                return timestamp
    return False

def current_hour_in_discharge_hours():
    current_hour = reset_time_to_hour()
    for timestamp in CHARGING_PLAN[0]['discharge_timestamps']:
        if isinstance(timestamp, datetime.datetime):
            if reset_time_to_hour(timestamp) == current_hour:
                return timestamp
    return False

def current_hour_in_force_discharge_hours():
    current_hour = reset_time_to_hour()
    for timestamp in CHARGING_PLAN[0]['force_discharge_timestamps'].keys():
        if isinstance(timestamp, datetime.datetime):
            if reset_time_to_hour(timestamp) == current_hour:
                return timestamp
    return False

def no_charging_modes_active():
    return False

@benchmark_decorator()
def charge_if_needed():
    func_name = "charge_if_needed"
    func_prefix = f"{func_name}_"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    global CHARGE_HOURS, TASKS
    
    try:
        if deactivate_script_enabled():
            _LOGGER.info("Script deactivated")
            set_charging_rule(f"⛔{i18n.t('ui.charge_if_needed.script_deactivated')}")
            return
        
        charging_rule = i18n.t('ui.charge_if_needed.not_charging')
        powerwall_action = "stopped"
        
        TASKS[f"{func_prefix}cheap_grid_charge_hours"] = task.create(cheap_grid_charge_hours)
        done, pending = task.wait({TASKS[f"{func_prefix}cheap_grid_charge_hours"]})
        
        to_timestamp = getTime()
        from_timestamp = to_timestamp - datetime.timedelta(minutes=CONFIG['cron_interval'])
        
        powerwall_watt_flow = int(get_average_value(CONFIG['home']['entity_ids']['powerwall_watt_flow_entity_id'], from_timestamp, to_timestamp, convert_to="W", error_state=0.0))
        inverter_watt_solar_only = get_state(CONFIG['solar']['entity_ids']['production_entity_id'], float_type=True, error_state=0.0)
        
        currentHour = reset_time_to_hour()
        
        if no_charging_modes_active():
            _LOGGER.info("No charging modes active, setting amps to max")
            amps = [CONFIG['charger']['charging_phases'], CONFIG['charger']['charging_max_amp']]
            charging_rule = f"⛔{i18n.t('ui.charge_if_needed.no_charging_modes_active', watt=int(amps[0] * amps[1] * CONFIG['charger']['power_voltage']))}"
        else:
            charge_hour = current_hour_in_charge_hours()
            discharge_hour = current_hour_in_discharge_hours()
            force_discharge_hour = current_hour_in_force_discharge_hours()
            
            if charge_hour:
                timestamp = charge_hour
                powerwall_action = "grid_charging"
                
                emoji = emoji_parse(CHARGE_HOURS[timestamp])
                charging_rule = i18n.t('ui.charge_if_needed.planned_charging', emoji=emoji)
                _LOGGER.info(f"Charging because of {emoji} {CHARGE_HOURS[timestamp]['Price']}{i18n.t('ui.common.valuta')}. ({MAX_KWH_CHARGING}kWh)")
            elif discharge_hour:
                timestamp = discharge_hour
                powerwall_action = "discharge_allowed"
                
                emoji = emoji_parse({'error': True})
                charging_rule = i18n.t('ui.charge_if_needed.discharge_allowed')
            elif force_discharge_hour:
                timestamp = force_discharge_hour
                powerwall_action = "force_discharge"
                
                emoji = emoji_parse({'error': True})
                charging_rule = i18n.t('ui.charge_if_needed.force_discharge')
            else:
                _LOGGER.info("No rules for charging")
                charging_rule = i18n.t('ui.charge_if_needed.not_charging')
                
        if powerwall_watt_flow != 0:
            if powerwall_watt_flow > 0 and CONFIG['home']['invert_powerwall_watt_flow_entity_id']:
                charging_rule += f"\n{i18n.t('ui.charge_if_needed.charging_watt', watt=int(powerwall_watt_flow))}{emoji_parse({'average': True})}"
            else:
                charging_rule += f"\n{i18n.t('ui.charge_if_needed.discharging_watt', watt=int(powerwall_watt_flow))}{emoji_parse({'average': True})}"
                
        set_charging_rule(charging_rule)
            
        set_state(f"sensor.{__name__}_powerwall_action", new_state=powerwall_action)
        
    except (asyncio.CancelledError, asyncio.TimeoutError, KeyError) as e:
        _LOGGER.error(f"Task cancelled {e} ({type(e)})")
    except Exception as e:
        global ERROR_COUNT
        
        error_message = f"Error running charge_if_needed(), setting charger and car to max: {e} {type(e)}"
        _LOGGER.error(error_message)
        my_persistent_notification(
            f"Error running charge_if_needed(), setting charger and car to max\nTrying to restart script to fix error in {ERROR_COUNT}/3: {e} {type(e)}",
            title=f"{TITLE} error",
            persistent_notification_id=f"{__name__}_{func_name}_restart_count_error"
        )
        
        if ERROR_COUNT == 3:
            save_error_to_file(error_message, caller_function_name = f"{func_name}()")
            _LOGGER.error(f"Restarting script to maybe fix error!!!")
            my_persistent_notification(
                f"Restarting script to maybe fix error!!!",
                title=f"{TITLE} error",
                persistent_notification_id=f"{__name__}_{func_name}_restart_script"
            )
            
            restart_script()
        else:
            _LOGGER.error(f"Trying to restart script to fix error in {ERROR_COUNT}/3")
            
        ERROR_COUNT += 1
                
        set_charging_rule(f"{emoji_parse({'error': True})}Fejl: Script deaktiveret, lader maks!!!")
    finally:
        task_cancel(func_prefix, task_remove=True, startswith=True)

def kwh_charged_by_solar():
    func_name = "kwh_charged_by_solar"
    func_prefix = f"{func_name}_"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    
    if not is_solar_configured(): return
    
    try:
        now = getTime()
        past = now - datetime.timedelta(minutes=60)
        
        TASKS[f"{func_prefix}watt"] = task.create(charge_from_powerwall, past, now)
        TASKS[f"{func_prefix}solar_watt"] = task.create(local_energy_available, period = 60, without_all_exclusion = True, solar_only = True)
        done, pending = task.wait({TASKS[f"{func_prefix}watt"], TASKS[f"{func_prefix}solar_watt"]})
        
        watt = TASKS[f"{func_prefix}watt"].result()
        solar_watt = TASKS[f"{func_prefix}solar_watt"].result()
        
        if not watt or not solar_watt:
            _LOGGER.error("No watt or solar_watt value, returning without setting kwh charged by solar")
            _LOGGER.error(f"watt: {watt}, solar_watt: {solar_watt}")
            return
        
        watt = round(abs(float(watt)), 3)
        solar_watt = round(max(float(solar_watt), 0.0), 3)
        
        solar_watt = min(solar_watt, watt)
        solar_kwh = round(solar_watt / 1000, 3)
        
        try:
            entity_kwh = get_state(entity_id=f"input_number.{__name__}_kwh_charged_by_solar", float_type=True, error_state=None)
            if entity_kwh is not None:
                entity_kwh = round(float(entity_kwh) + solar_kwh, 2)
                set_state(entity_id=f"input_number.{__name__}_kwh_charged_by_solar", new_state=entity_kwh)
            else:
                raise Exception(f"Cant get state for input_number.{__name__}_kwh_charged_by_solar")
        except Exception as e:
            _LOGGER.error(e)
            my_persistent_notification(
                f"Cant set input_number.{__name__}_kwh_charged_by_solar: {e} {type(e)}",
                title=f"{TITLE} error",
                persistent_notification_id=f"{__name__}_{func_name}_set_state_error"
            )
    except (asyncio.CancelledError, asyncio.TimeoutError, KeyError) as e:
        _LOGGER.error(f"Task was cancelled or timed out: {e} {type(e)}")
        my_persistent_notification(
            f"Task was cancelled or timed out: {e} {type(e)}",
            title=f"{TITLE} error",
            persistent_notification_id=f"{__name__}_{func_name}_task_error"
        )
    except Exception as e:
        _LOGGER.error(f"Error calculating kwh charged by solar: {e} {type(e)}")
        my_persistent_notification(
            f"Error calculating kwh charged by solar: {e} {type(e)}",
            title=f"{TITLE} error",
            persistent_notification_id=f"{__name__}_{func_name}_error"
        )
    finally:
        task_cancel(func_prefix, task_remove=True, startswith=True)

def calc_local_energy_kwh(from_timestamp, to_timestamp, kwh = None, solar_period_current_hour = False):
    func_name = "calc_local_energy_kwh"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    
    if kwh == 0.0 or kwh is None:
        return 0.0, 0.0
    
    watt = kwh * 1000.0

    watts_available_from_local_energy, watts_from_local_energy, solar_watts_of_local_energy = local_energy_available(from_timestamp=from_timestamp, to_timestamp=to_timestamp, include_local_energy_distribution = True, without_all_exclusion = True)
    
    _LOGGER.info(f"watt:{watt} watts_available_from_local_energy:{watts_available_from_local_energy} watts_from_local_energy:{watts_from_local_energy} solar_watts_of_local_energy:{solar_watts_of_local_energy}")
    if watts_from_local_energy > watt:
        miscalculated_ratio = watts_from_local_energy / watt if watt > 0.0 else 0.0
        solar_watts_of_local_energy = solar_watts_of_local_energy * miscalculated_ratio
        watts_from_local_energy = watt
    
    solar_kwh_available = round(max(watts_from_local_energy, 0.0) / 1000, 3)
    solar_kwh_of_local_energy = round(solar_watts_of_local_energy / 1000, 3)
    _LOGGER.info(f"calc_solar_kwh called with from_timestamp:{from_timestamp} to_timestamp:{to_timestamp} kwh:{kwh} solar_kwh_available:{solar_kwh_available} solar_kwh_of_local_energy:{solar_kwh_of_local_energy}")

    return round(min(solar_kwh_available, kwh), 3), solar_kwh_of_local_energy

def load_kwh_prices():
    func_name = "load_kwh_prices"
    func_prefix = f"{func_name}_"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    global KWH_AVG_PRICES_DB
        
    def set_default_values(name):
        global KWH_AVG_PRICES_DB
        nonlocal force_save
        if name not in KWH_AVG_PRICES_DB:
            KWH_AVG_PRICES_DB[name] = {}
            
        for h in range(24):
            if h not in KWH_AVG_PRICES_DB[name]:
                KWH_AVG_PRICES_DB[name][h] = {}
                
            for d in range(7):
                if d not in KWH_AVG_PRICES_DB[name][h]:
                    KWH_AVG_PRICES_DB[name][h][d] = []
                    
        force_save = True
    
    version = 1.0
    force_save = False
    
    try:
        filename = f"{__name__}_kwh_avg_prices_db"
        TASKS[f'{func_prefix}load_yaml'] = task.create(load_yaml, filename)
        done, pending = task.wait({TASKS[f'{func_prefix}load_yaml']})
        database = TASKS[f'{func_prefix}load_yaml'].result()
        
        if "version" in database:
            version = float(database["version"])
            del database["version"]
        
        KWH_AVG_PRICES_DB = deepcopy(database)

        if not KWH_AVG_PRICES_DB:
            TASKS[f'{func_prefix}create_yaml'] = task.create(create_yaml, filename, db=KWH_AVG_PRICES_DB)
            done, pending = task.wait({TASKS[f'{func_prefix}create_yaml']})
    except (asyncio.CancelledError, asyncio.TimeoutError, KeyError) as e:
        _LOGGER.warning(f"Task was cancelled or timed out: {e} {type(e)}")
    except Exception as e:
        error_message = f"Error loading {__name__}_kwh_avg_prices_db: {e} {type(e)}"
        _LOGGER.error(error_message)
        save_error_to_file(error_message, caller_function_name = f"{func_name}()")
        my_persistent_notification(
            f"Cant load {__name__}_kwh_avg_prices_db: {e} {type(e)}",
            title=f"{TITLE} error",
            persistent_notification_id=f"{__name__}_{func_name}_load_error"
        )
    finally:
        task_cancel(func_prefix, task_remove=True, startswith=True)
    
    if KWH_AVG_PRICES_DB == {} or not KWH_AVG_PRICES_DB:
        KWH_AVG_PRICES_DB = {}
        
    for name in ("history", "history_sell"):
        if name not in KWH_AVG_PRICES_DB:
            version = 2.0
            set_default_values(name)
    
    for name in ("max", "mean", "min"):
        if name not in KWH_AVG_PRICES_DB:
            KWH_AVG_PRICES_DB[name] = []
            force_save = True
    
def save_kwh_prices():
    global KWH_AVG_PRICES_DB
    
    if len(KWH_AVG_PRICES_DB) > 0:
        db_to_file = deepcopy(KWH_AVG_PRICES_DB)
        db_to_file["version"] = KWH_AVG_PRICES_DB_VERSION
        save_changes(f"{__name__}_kwh_avg_prices_db", db_to_file)

def append_kwh_prices():
    func_name = "append_kwh_prices"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    global KWH_AVG_PRICES_DB
    
    if len(KWH_AVG_PRICES_DB) == 0:
        load_kwh_prices()
    
    if CONFIG['prices']['entity_ids']['power_prices_entity_id'] in state.names(domain="sensor"):
        power_prices_attr = get_attr(CONFIG['prices']['entity_ids']['power_prices_entity_id'], error_state={})
        
        if "today" not in power_prices_attr:
            _LOGGER.error(f"Power prices entity {CONFIG['prices']['entity_ids']['power_prices_entity_id']} does not have 'today' attribute")
            my_persistent_notification(
                f"Power prices entity {CONFIG['prices']['entity_ids']['power_prices_entity_id']} does not have 'today' attribute",
                title=f"{TITLE} error",
                persistent_notification_id=f"{__name__}_{func_name}_no_today_attr"
            )
            return
        
        today = power_prices_attr["today"]
        
        max_price = max(today) - get_refund()
        mean_price = round(average(today), 3) - get_refund()
        min_price = min(today) - get_refund()
        
        max_length = CONFIG['database']['kwh_avg_prices_db_data_to_save']
        
        KWH_AVG_PRICES_DB['max'].insert(0, max_price)
        KWH_AVG_PRICES_DB['mean'].insert(0, mean_price)
        KWH_AVG_PRICES_DB['min'].insert(0, min_price)
        KWH_AVG_PRICES_DB['max'] = KWH_AVG_PRICES_DB['max'][:max_length]
        KWH_AVG_PRICES_DB['mean'] = KWH_AVG_PRICES_DB['mean'][:max_length]
        KWH_AVG_PRICES_DB['min'] = KWH_AVG_PRICES_DB['min'][:max_length]
    
        transmissions_nettarif = 0.0
        systemtarif = 0.0
        elafgift = 0.0
        
        if "tariffs" in power_prices_attr:
            attr = power_prices_attr["tariffs"]
            transmissions_nettarif = attr["additional_tariffs"]["transmissions_nettarif"]
            systemtarif = attr["additional_tariffs"]["systemtarif"]
            elafgift = attr["additional_tariffs"]["elafgift"]
            
        day_of_week = getDayOfWeek()
        
        for h in range(24):
            KWH_AVG_PRICES_DB['history'][h][day_of_week].insert(0, today[h] - get_refund())
            KWH_AVG_PRICES_DB['history'][h][day_of_week] = KWH_AVG_PRICES_DB['history'][h][day_of_week][:max_length]
            
            if is_solar_configured():
                tariffs = 0.0
                
                if "tariffs" in power_prices_attr:
                    tariffs = attr["tariffs"][str(h)]
                    
                tariff_sum = sum([transmissions_nettarif, systemtarif, elafgift, tariffs])
                raw_price = today[h] - tariff_sum

                energinets_network_tariff = SOLAR_SELL_TARIFF["energinets_network_tariff"]
                energinets_balance_tariff = SOLAR_SELL_TARIFF["energinets_balance_tariff"]
                solar_production_seller_cut = SOLAR_SELL_TARIFF["solar_production_seller_cut"]
                
                sell_tariffs = sum((solar_production_seller_cut, energinets_network_tariff, energinets_balance_tariff, transmissions_nettarif, systemtarif))
                sell_price = raw_price - sell_tariffs
                    
                sell_price = round(sell_price, 3)
                KWH_AVG_PRICES_DB['history_sell'][h][day_of_week].insert(0, sell_price)
                KWH_AVG_PRICES_DB['history_sell'][h][day_of_week] = KWH_AVG_PRICES_DB['history_sell'][h][day_of_week][:max_length]
        
        save_kwh_prices()

if INITIALIZATION_COMPLETE:
    if DEFAULT_ENTITIES.get("input_select", {}).get("battmind_select_release", {}):
        @service(f"pyscript.{__name__}_update_release_options")
        @state_trigger(f"input_select.battmind_select_release")
        def update_release_options(trigger_type=None, trigger_id=None, var_name=None, value=None, old_value=None, **kwargs):
            """yaml
            name: "BattMind: Update Release Options"
            description: >
                Fetches all release tags from GitHub and updates an input_select
                entity with them, so the user can pick a release manually.
            """
            func_name = "update_release_options"
            func_prefix = f"{func_name}_"
            _LOGGER = globals()['_LOGGER'].getChild(func_name)
            global TASKS
            
            entity_id="input_select.battmind_select_release"
            repo_api = "https://api.github.com/repos/dezito/BattMind/releases"
            
            try:
                
                if trigger_type == "state":
                    TASKS[f'{func_prefix}check_release_updates'] = task.create(check_release_updates)
                    done, pending = task.wait({TASKS[f'{func_prefix}check_release_updates']})
                    return
                
                _LOGGER.info("Fetching release list from GitHub...")
                TASKS[f'{func_prefix}_fetch'] = task.create(run_console_command_sync, ["curl", "-s", repo_api])
                done, pending = task.wait({TASKS[f'{func_prefix}_fetch']})
                api_response = TASKS[f'{func_prefix}_fetch'].result()

                releases = json.loads(api_response)

                # extract tag names (newest first)
                tags = [r.get("tag_name") for r in releases if r.get("tag_name")]
                if not tags:
                    raise ValueError("No tags found in release data")

                tags.insert(0, "latest")  # add 'latest' option at the top
                _LOGGER.info(f"Found {len(tags)} releases: {tags}")
                
                # update input_select options
                input_select.set_options(entity_id=entity_id, options=tags)

                # optionally set the first (latest) as current value
                input_select.select_option(entity_id=entity_id, option=tags[0])
                
                if trigger_type == "service":
                    my_persistent_notification(
                        f"✅ Release list updated ({len(tags)} options)",
                        title=f"{TITLE} Release Selector",
                        persistent_notification_id=f"{__name__}_{func_name}"
                    )
            except (asyncio.CancelledError, asyncio.TimeoutError, KeyError) as e:
                _LOGGER.warning(f"Task cancelled or timeout: {e} {type(e)}")
                return
            except Exception as e:
                _LOGGER.error(f"Failed to update release list: {e}")
                my_persistent_notification(
                    f"⚠️ Failed to update release list: {e}",
                    title=f"{TITLE} Release Selector Error",
                    persistent_notification_id=f"{__name__}_{func_name}"
                )
            finally:
                task_cancel(func_prefix, task_remove=True, timeout=5.0, startswith=True)
                
    @benchmark_decorator()
    @time_trigger("startup")
    def startup(trigger_type=None, var_name=None, value=None, old_value=None):
        func_name = "startup"
        func_prefix = f"{func_name}_"
        _LOGGER = globals()['_LOGGER'].getChild(func_name)
        global TASKS
        
        log_lines = []
        try:
            TASKS[f"{func_prefix}validation_procedure"] = task.create(validation_procedure)
            done, pending = task.wait({TASKS[f"{func_prefix}validation_procedure"]})
            
            solar_configured = i18n.t('ui.startup.configuration.configured') if is_solar_configured() else i18n.t('ui.startup.configuration.not_configured')
            powerwall_configured = i18n.t('ui.startup.configuration.configured') if is_powerwall_configured() else i18n.t('ui.startup.configuration.not_configured')
            
            log_lines.append(f"---")
            log_lines.append(f"### {welcome()}")
            log_lines.append(f"")
            log_lines.append(f"**{i18n.t('ui.startup.configuration.header')}:**")
            log_lines.append(f"📟{i18n.t('ui.startup.configuration.solar')}: {solar_configured}")
            log_lines.append(f"📟{i18n.t('ui.startup.configuration.powerwall')}: {powerwall_configured}")
            log_lines.append(f"")
            set_charging_rule(f"📟{i18n.t('ui.startup.setting_entities')}")
            log_lines.append(f"📟{i18n.t('ui.startup.setting_entities')}")
            
            TASKS[f"{func_prefix}set_entity_friendlynames"] = task.create(set_entity_friendlynames)
            TASKS[f"{func_prefix}emoji_description"] = task.create(emoji_description)
            TASKS[f"{func_prefix}set_default_entity_states"] = task.create(set_default_entity_states)
            done, pending = task.wait({TASKS[f"{func_prefix}set_entity_friendlynames"], TASKS[f"{func_prefix}emoji_description"], TASKS[f"{func_prefix}set_default_entity_states"]})
            
            set_charging_rule(f"📟{i18n.t('ui.startup.loading_db')}")
            log_lines.append(f"📟{i18n.t('ui.startup.loading_db')}")
            
            TASKS[f"{func_prefix}update_grid_prices"] = task.create(update_grid_prices)
            TASKS[f"{func_prefix}load_power_values_db"] = task.create(load_power_values_db)
            TASKS[f"{func_prefix}load_solar_available_db"] = task.create(load_solar_available_db)
            TASKS[f"{func_prefix}load_kwh_prices"] = task.create(load_kwh_prices)
            done, pending = task.wait({TASKS[f"{func_prefix}update_grid_prices"], TASKS[f"{func_prefix}load_power_values_db"], TASKS[f"{func_prefix}load_solar_available_db"], TASKS[f"{func_prefix}load_kwh_prices"]})
            
            log_lines.append(f"📟{i18n.t('ui.startup.loading_history')}")
            
            TASKS[f"{func_prefix}load_charging_history"] = task.create(load_charging_history)
            done, pending = task.wait({TASKS[f"{func_prefix}load_charging_history"]})
            
            log_lines.append(f"📟{i18n.t('ui.startup.almost_ready')}")
            
            log_lines.append(f"")
            
            log_lines.append(f"📟{i18n.t('ui.startup.calc_charging_plan')}")
            set_charging_rule(f"📟{i18n.t('ui.startup.calc_charging_plan')}")
            
            task_cancel("charge_if_needed", task_remove=True, contains=True)
            TASKS[f"{func_prefix}charge_if_needed"] = task.create(charge_if_needed)
            done, pending = task.wait({TASKS[f"{func_prefix}charge_if_needed"]})
            
            if DEFAULT_ENTITIES.get("input_select", {}).get("battmind_select_release", {}):
                if CONFIG['notification']['update_available']:
                    TASKS[f"{func_prefix}check_release_updates"] = task.create(check_release_updates)
                    TASKS[f"{func_prefix}update_release_options"] = task.create(update_release_options)
                    done, pending = task.wait({TASKS[f"{func_prefix}check_release_updates"], TASKS[f"{func_prefix}update_release_options"]})
                else:
                    TASKS[f"{func_prefix}update_release_options"] = task.create(update_release_options)
                    done, pending = task.wait({TASKS[f"{func_prefix}update_release_options"]})
                    
            current_hour = reset_time_to_hour()
            TASKS[f'{func_prefix}charging_history'] = task.create(charging_history, timestamp=current_hour)
            
            done, pending = task.wait({TASKS[f'{func_prefix}charging_history']})
        except Exception as e:
            _LOGGER.error(f"Error during startup: {e} {type(e)}")
            my_persistent_notification(
                f"Error during startup: {e} {type(e)}",
                title=f"{TITLE} error",
                persistent_notification_id=f"{__name__}_{func_name}_error"
            )
        finally:
            for line in log_lines:
                _LOGGER.info(line)
                
            log_lines.append(f"\n<center>\n\n**{i18n.t('ui.startup.call_all_entities')}**\n\n</center>")
            
            my_persistent_notification(
                f"{"\n".join(log_lines)}",
                f"📟{TITLE} started",
                persistent_notification_id=f"{__name__}_{func_name}_notification"
            )
            
            task_cancel(func_prefix, task_remove=True, startswith=True)
        
        append_overview_output(f"📟{BASENAME} started")
        TASKS[f"{func_prefix}debug_info"] = task.create(debug_info)
        done, pending = task.wait({TASKS[f"{func_prefix}debug_info"]})
    
    @state_trigger(f"input_boolean.{__name__}_deactivate_script")
    def charging_rule_changed(trigger_type=None, var_name=None, value=None, old_value=None):
        func_name = "charging_rule_changed"
        func_prefix = f"{func_name}_"
        _LOGGER = globals()['_LOGGER'].getChild(func_name)
        global TASKS
        
        if var_name == f"input_boolean.{__name__}_deactivate_script":
            try:
                task_cancel("charge_if_needed", task_remove=True, contains=True)
                TASKS[f"{func_prefix}charge_if_needed"] = task.create(charge_if_needed)
                done, pending = task.wait({TASKS[f"{func_prefix}charge_if_needed"]})
            except (asyncio.CancelledError, asyncio.TimeoutError, KeyError) as e:
                _LOGGER.warning(f"Task was cancelled or timed out in {func_name} for {var_name}: {e} {type(e)}")
            except ValueError as ve:
                _LOGGER.error(f"Value error in {func_name} for {var_name}: {ve} {type(ve)}")
            except Exception as e:
                _LOGGER.error(f"Error in {func_name} for {var_name}: {e} {type(e)}")
            finally:
                task_cancel(func_prefix, task_remove=True, startswith=True)
                
                    
                
    @time_trigger(f"cron(0/{CONFIG['cron_interval']} * * * *)")
    @state_trigger(f"input_button.{__name__}_enforce_planning")
    def triggers_charge_if_needed(trigger_type=None, var_name=None, value=None, old_value=None):
        func_name = "triggers_charge_if_needed"
        func_prefix = f"{func_name}_"
        _LOGGER = globals()['_LOGGER'].getChild(func_name)
        global TASKS
        try:
            if deactivate_script_enabled():
                return
            
            task_cancel("charge_if_needed", task_remove=True, contains=True)
            TASKS[f"{func_prefix}charge_if_needed"] = task.create(charge_if_needed)
            done, pending = task.wait({TASKS[f"{func_prefix}charge_if_needed"]})
            
            if var_name == f"input_button.{__name__}_enforce_planning" and TESTING:
                TASKS[f"{func_prefix}debug_info"] = task.create(debug_info)
                done, pending = task.wait({TASKS[f"{func_prefix}debug_info"]})
        except (asyncio.CancelledError, asyncio.TimeoutError, KeyError) as e:
            _LOGGER.warning(f"Task was cancelled or timed out in {func_name}: {e} {type(e)}")
        except Exception as e:
            _LOGGER.error(f"Error in {func_name}: {e} {type(e)}")
            my_persistent_notification(
                f"Error in {func_name}: {e} {type(e)}",
                title=f"{TITLE} error",
                persistent_notification_id=f"{__name__}_{func_name}_error"
            )
        finally:
            task_cancel(func_prefix, task_remove=True, startswith=True)
    
    @time_trigger(f"cron(59 * * * *)")
    def cron_hour_end(trigger_type=None, var_name=None, value=None, old_value=None):
        func_name = "cron_hour_end"
        func_prefix = f"{func_name}_"
        _LOGGER = globals()['_LOGGER'].getChild(func_name)
        global TASKS
        
        current_hour = reset_time_to_hour()
        
        try:
            TASKS[f'{func_prefix}local_energy_available'] = task.create(local_energy_available, period = 60, without_all_exclusion = True, solar_only = True)
            TASKS[f'{func_prefix}power_values'] = task.create(power_values, period = 60)
            done, pending = task.wait({TASKS[f'{func_prefix}local_energy_available'], TASKS[f'{func_prefix}power_values']})
            
            cron_local_energy_available = TASKS[f'{func_prefix}local_energy_available'].result()
            cron_power_values = TASKS[f'{func_prefix}power_values'].result()
            
            TASKS[f'{func_prefix}solar_available_append_to_db'] = task.create(solar_available_append_to_db, cron_local_energy_available)
            TASKS[f'{func_prefix}power_values_to_db'] = task.create(power_values_to_db, cron_power_values)
            TASKS[f'{func_prefix}kwh_charged_by_solar'] = task.create(kwh_charged_by_solar)
            TASKS[f'{func_prefix}charging_history'] = task.create(charging_history, timestamp=current_hour)
            
            done, pending = task.wait({TASKS[f'{func_prefix}solar_available_append_to_db'], TASKS[f'{func_prefix}power_values_to_db'],
                                       TASKS[f'{func_prefix}kwh_charged_by_solar'], TASKS[f'{func_prefix}charging_history']})

            try:
                charging_history_result = TASKS[f"{func_prefix}charging_history"].result()
            except Exception as e:
                _LOGGER.warning(f"Charging history task threw: {e}. Sleeping 10s and retrying once...")
                task.wait_until(timeout=10)

                TASKS[f"{func_prefix}charging_history"] = task.create(charging_history, timestamp=current_hour)
                task.wait({TASKS[f"{func_prefix}charging_history"]})
                charging_history_result = TASKS[f"{func_prefix}charging_history"].result()

            if not charging_history_result:
                _LOGGER.warning(f"Charging history returned no result for {current_hour} (None/empty).")
            else:
                _LOGGER.info(f"Charging history ok for {current_hour}")
            
        except (asyncio.CancelledError, asyncio.TimeoutError, KeyError) as e:
            _LOGGER.warning(f"Task was cancelled or timed out in {func_name}: {e} {type(e)}")
        except Exception as e:
            _LOGGER.error(f"Error in {func_name}: {e} {type(e)}")
            my_persistent_notification(
                f"Error in {func_name}: {e} {type(e)}",
                title=f"{TITLE} error",
                persistent_notification_id=f"{__name__}_{func_name}_error"
            )
        finally:
            task_cancel(func_prefix, task_remove=True, startswith=True)
        
    @time_trigger(f"cron(0 0 * * *)")
    def cron_new_day(trigger_type=None, var_name=None, value=None, old_value=None):
        func_name = "cron_new_day"
        func_prefix = f"{func_name}_"
        _LOGGER = globals()['_LOGGER'].getChild(func_name)
        global TASKS
        
        try:
            if DEFAULT_ENTITIES.get("input_select", {}).get("battmind_select_release", {}):
                if CONFIG['notification']['update_available']:
                    TASKS[f"{func_prefix}check_release_updates"] = task.create(check_release_updates)
                    TASKS[f"{func_prefix}update_release_options"] = task.create(update_release_options)
                    done, pending = task.wait({TASKS[f"{func_prefix}check_release_updates"], TASKS[f"{func_prefix}update_release_options"]})
                else:
                    TASKS[f"{func_prefix}update_release_options"] = task.create(update_release_options)
                    done, pending = task.wait({TASKS[f"{func_prefix}update_release_options"]})
        except (asyncio.CancelledError, asyncio.TimeoutError, KeyError) as e:
            _LOGGER.warning(f"Task was cancelled or timed out in {func_name}: {e} {type(e)}")
        except Exception as e:
            _LOGGER.error(f"Error in {func_name}: {e} {type(e)}")
            my_persistent_notification(
                f"Error in {func_name}: {e} {type(e)}",
                title=f"{TITLE} error",
                persistent_notification_id=f"{__name__}_{func_name}_error"
            )
        finally:
            task_cancel(func_prefix, task_remove=True, startswith=True)
            
    @time_trigger(f"cron(2 * * * *)")
    def cron_update_grid_prices(trigger_type=None, var_name=None, value=None, old_value=None):
        func_name = "cron_update_grid_prices"
        func_prefix = f"{func_name}_"
        _LOGGER = globals()['_LOGGER'].getChild(func_name)
        global TASKS
        
        try:
            TASKS[f"{func_prefix}update_grid_prices"] = task.create(update_grid_prices)
            done, pending = task.wait({TASKS[f"{func_prefix}update_grid_prices"]})
        except (asyncio.CancelledError, asyncio.TimeoutError, KeyError) as e:
            _LOGGER.warning(f"Task was cancelled or timed out in {func_name}: {e} {type(e)}")
        except Exception as e:
            _LOGGER.error(f"Error in {func_name}: {e} {type(e)}")
            my_persistent_notification(
                f"Error in {func_name}: {e} {type(e)}",
                title=f"{TITLE} error",
                persistent_notification_id=f"{__name__}_{func_name}_error"
            )
        finally:
            task_cancel(func_prefix, task_remove=True, startswith=True)
        
    @time_trigger(f"cron(0 5 * * *)")
    def cron_append_kwh_prices(trigger_type=None, var_name=None, value=None, old_value=None):
        func_name = "cron_append_kwh_prices"
        func_prefix = f"{func_name}_"
        _LOGGER = globals()['_LOGGER'].getChild(func_name)
        global TASKS
        
        try:
            TASKS[f"{func_prefix}append_kwh_prices"] = task.create(append_kwh_prices)
            done, pending = task.wait({TASKS[f"{func_prefix}append_kwh_prices"]})
        except (asyncio.CancelledError, asyncio.TimeoutError, KeyError) as e:
            _LOGGER.warning(f"Task was cancelled or timed out in {func_name}: {e} {type(e)}")
        except Exception as e:
            _LOGGER.error(f"Error in {func_name}: {e} {type(e)}")
            my_persistent_notification(
                f"Error in {func_name}: {e} {type(e)}",
                title=f"{TITLE} error",
                persistent_notification_id=f"{__name__}_{func_name}_error"
            )
        finally:
            task_cancel(func_prefix, task_remove=True, startswith=True)

    @time_trigger("shutdown")
    def shutdown(trigger_type=None, var_name=None, value=None, old_value=None):
        func_name = "shutdown"
        func_prefix = f"{func_name}_"
        _LOGGER = globals()['_LOGGER'].getChild(func_name)
        
        global CONFIG, TASKS
        
        current_mod_time = get_file_modification_time(f"{__name__}_config.yaml")
        if CONFIG_LAST_MODIFIED != current_mod_time:
            script_loaded__mod_datetime = datetime.datetime.fromtimestamp(CONFIG_LAST_MODIFIED).strftime('%Y-%m-%d %H:%M:%S')
            current_config_mod_datetime = datetime.datetime.fromtimestamp(current_mod_time).strftime('%Y-%m-%d %H:%M:%S')
            _LOGGER.info(f"Config file has been modified since script start, reloading config to avoid overwriting changes made outside the script. Script loaded: {script_loaded__mod_datetime}, Current config: {current_config_mod_datetime}")
            
            TASKS[f'{func_prefix}load_yaml'] = task.create(load_yaml, f"{__name__}_config.yaml")
            done, pending = task.wait({TASKS[f'{func_prefix}load_yaml']})
            cfg_temp = TASKS[f'{func_prefix}load_yaml'].result()
            
            if cfg_temp:
                CONFIG = cfg_temp
            
        set_charging_rule(f"📟{i18n.t('ui.shutdown')}")
        try:
            CONFIG['prices']['cheap_price_periods'] = get_cheap_price_periods()
            CONFIG['prices']['cheap_price_period_rise_threshold'] = get_cheap_price_period_rise_threshold()
            CONFIG['prices']['cheapest_price_rise_threshold'] = get_cheapest_price_rise_threshold()
            
            if is_solar_configured():
                CONFIG['solar']['production_price'] = float(get_state(f"input_number.{__name__}_solar_sell_fixed_price", float_type=True, error_state=CONFIG['solar']['production_price']))
            
            TASKS[f'{func_prefix}save_changes'] = task.create(save_changes, f"{__name__}_config", CONFIG)
            done, pending = task.wait({TASKS[f'{func_prefix}save_changes']})
        except Exception as e:
            _LOGGER.error(f"Cant save config from Home assistant to config: {e} {type(e)}")
            my_persistent_notification(
                f"Cant save config from Home assistant to config: {e} {type(e)}",
                title=f"{TITLE} Error",
                persistent_notification_id=f"{__name__}_{func_name}_error")
        
        task.wait_until(timeout=1.0)
        task_shutdown()
            
@state_trigger(f"input_button.{__name__}_restart_script")
def restart(trigger_type=None, var_name=None, value=None, old_value=None):
    func_name = "restart"
    _LOGGER = globals()['_LOGGER'].getChild(func_name)
    restart_script()
