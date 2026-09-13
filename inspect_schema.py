import sqlite3

DB = "railway.db"
conn = sqlite3.connect(DB)

def _is_num(v):
    try:
        float(str(v).replace(',', '').strip())
        return True
    except Exception:
        return False

tables = {
    "vertical_wear_data": ["max_vertical_wear_mm", "average_vertical_wear_mm", "start_location_km"],
    "lateral_wear_data":  ["max_wear_mm", "average_lateral_wear_mm", "start_location_km"],
    "sleeper_defects_data": ["nos_of_affected_sleepers_broken_sleeper",
                             "nos_of_affected_sleepers_cracked_sleeper_2mm_100mm",
                             "location_km"],
    "sod_data":           ["size_of_obstacle_mm_l", "size_of_obstacle_mm_b",
                           "size_of_obstacle_mm_h", "location_km"],
    "lip_flow_data":      ["max_value_mm", "average_value_mm", "start_location_km"],
    "fittings_data":      ["component_defects_left_rail_missing_loose_clip",
                           "component_defects_left_rail_missing_bolt_and_nut", "location_km"],
    "rail_defects_data":  ["value_of_defect_mm", "location_km"],
    "ballast_vegetation_data": ["length_of_track_having_m_defect_length", "location_start_location_km"],
}

for tbl, cols in tables.items():
    print(f"\n=== {tbl} ===")
    for col in cols:
        try:
            rows = conn.execute(
                f"SELECT [{col}] FROM [{tbl}] WHERE [{col}] IS NOT NULL AND [{col}] != '' LIMIT 8"
            ).fetchall()
            vals = [r[0] for r in rows]
            bad = [v for v in vals if not _is_num(v)]
            marker = "  *** NON-NUMERIC ***" if bad else ""
            print(f"  {col}: {vals}{marker}")
        except Exception as ex:
            print(f"  {col}: ERROR {ex}")

conn.close()
