# Grid-Specific User Defined Functions

from pyspark.sql.types import DoubleType, StringType, IntegerType
from pyspark.sql import functions as F
import math


@F.udf(returnType=DoubleType())
def calculate_power_loss(voltage_kv: float, current_a: float, resistance_ohm: float) -> float:
    """
    Calculate power loss in transmission line.
    
    Formula: P_loss = I^2 * R
    
    Args:
        voltage_kv: Voltage in kilovolts
        current_a: Current in amperes
        resistance_ohm: Resistance in ohms
        
    Returns:
        Power loss in kilowatts
    """
    if current_a is None or resistance_ohm is None:
        return None
    
    # Power loss = I^2 * R (in watts), convert to kW
    power_loss_w = (current_a ** 2) * resistance_ohm
    return power_loss_w / 1000.0


@F.udf(returnType=DoubleType())
def calculate_apparent_power(voltage_kv: float, current_a: float) -> float:
    """
    Calculate apparent power (S = V * I).
    
    Args:
        voltage_kv: Voltage in kilovolts
        current_a: Current in amperes
        
    Returns:
        Apparent power in MVA (megavolt-amperes)
    """
    if voltage_kv is None or current_a is None:
        return None
    
    # Convert kV to V, calculate VA, then convert to MVA
    apparent_power_va = (voltage_kv * 1000) * current_a
    return apparent_power_va / 1_000_000.0


@F.udf(returnType=DoubleType())
def calculate_power_factor(active_power_mw: float, apparent_power_mva: float) -> float:
    """
    Calculate power factor (PF = P / S).
    
    Args:
        active_power_mw: Active power in megawatts
        apparent_power_mva: Apparent power in MVA
        
    Returns:
        Power factor (0-1)
    """
    if active_power_mw is None or apparent_power_mva is None or apparent_power_mva == 0:
        return None
    
    pf = active_power_mw / apparent_power_mva
    # Clamp between 0 and 1
    return max(0.0, min(1.0, pf))


@F.udf(returnType=StringType())
def classify_voltage_level(voltage_kv: float) -> str:
    """
    Classify voltage into standard grid levels.
    
    Args:
        voltage_kv: Voltage in kilovolts
        
    Returns:
        Voltage level classification
    """
    if voltage_kv is None:
        return "unknown"
    
    if voltage_kv < 1:
        return "low_voltage"  # < 1 kV
    elif voltage_kv < 36:
        return "medium_voltage"  # 1-36 kV
    elif voltage_kv < 150:
        return "high_voltage"  # 36-150 kV
    else:
        return "extra_high_voltage"  # > 150 kV


@F.udf(returnType=DoubleType())
def calculate_frequency_deviation(frequency_hz: float, nominal_hz: float = 50.0) -> float:
    """
    Calculate frequency deviation from nominal (50 Hz in Europe).
    
    Args:
        frequency_hz: Measured frequency
        nominal_hz: Nominal frequency (default 50 Hz)
        
    Returns:
        Deviation in Hz
    """
    if frequency_hz is None:
        return None
    
    return frequency_hz - nominal_hz


@F.udf(returnType=IntegerType())
def is_frequency_critical(frequency_hz: float, tolerance: float = 0.2) -> int:
    """
    Check if frequency is outside critical tolerance.
    
    European grid standard: 50 Hz ± 0.2 Hz normal operating range
    
    Args:
        frequency_hz: Measured frequency
        tolerance: Tolerance in Hz (default 0.2)
        
    Returns:
        1 if critical, 0 if normal
    """
    if frequency_hz is None:
        return None
    
    deviation = abs(frequency_hz - 50.0)
    return 1 if deviation > tolerance else 0


@F.udf(returnType=DoubleType())
def calculate_haversine_distance(lat1: float, lon1: float, 
                                 lat2: float, lon2: float) -> float:
    """
    Calculate distance between two geographic points using Haversine formula.
    
    Args:
        lat1, lon1: Coordinates of first point
        lat2, lon2: Coordinates of second point
        
    Returns:
        Distance in kilometers
    """
    if None in [lat1, lon1, lat2, lon2]:
        return None
    
    # Earth radius in kilometers
    R = 6371.0
    
    # Convert to radians
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)
    
    # Haversine formula
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    
    a = math.sin(dlat / 2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    
    distance = R * c
    return distance


@F.udf(returnType=DoubleType())
def calculate_load_factor(avg_load_mw: float, peak_load_mw: float) -> float:
    """
    Calculate load factor (ratio of average to peak load).
    
    Args:
        avg_load_mw: Average load in MW
        peak_load_mw: Peak load in MW
        
    Returns:
        Load factor (0-1)
    """
    if avg_load_mw is None or peak_load_mw is None or peak_load_mw == 0:
        return None
    
    load_factor = avg_load_mw / peak_load_mw
    return max(0.0, min(1.0, load_factor))


@F.udf(returnType=StringType())
def get_grid_stability_status(frequency_hz: float, voltage_kv: float, 
                              nominal_voltage_kv: float) -> str:
    """
    Assess overall grid stability status based on key parameters.
    
    Args:
        frequency_hz: Measured frequency
        voltage_kv: Measured voltage
        nominal_voltage_kv: Nominal voltage for comparison
        
    Returns:
        Stability status: stable, warning, critical
    """
    if None in [frequency_hz, voltage_kv, nominal_voltage_kv]:
        return "unknown"
    
    # Check frequency deviation
    freq_deviation = abs(frequency_hz - 50.0)
    
    # Check voltage deviation (as percentage)
    voltage_deviation_pct = abs((voltage_kv - nominal_voltage_kv) / nominal_voltage_kv) * 100
    
    # Determine status
    if freq_deviation > 0.5 or voltage_deviation_pct > 10:
        return "critical"
    elif freq_deviation > 0.2 or voltage_deviation_pct > 5:
        return "warning"
    else:
        return "stable"


def register_udfs(spark):
    """
    Register all UDFs with the Spark session.
    
    Usage:
        from src.udfs.grid_udfs import register_udfs
        register_udfs(spark)
    """
    spark.udf.register("calculate_power_loss", calculate_power_loss)
    spark.udf.register("calculate_apparent_power", calculate_apparent_power)
    spark.udf.register("calculate_power_factor", calculate_power_factor)
    spark.udf.register("classify_voltage_level", classify_voltage_level)
    spark.udf.register("calculate_frequency_deviation", calculate_frequency_deviation)
    spark.udf.register("is_frequency_critical", is_frequency_critical)
    spark.udf.register("calculate_haversine_distance", calculate_haversine_distance)
    spark.udf.register("calculate_load_factor", calculate_load_factor)
    spark.udf.register("get_grid_stability_status", get_grid_stability_status)
    
    print("✅ Grid UDFs registered successfully")
