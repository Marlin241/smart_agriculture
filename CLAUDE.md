# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Academic Big Data project simulating a smart farm in **Webots R2025a**. Four field robots monitor and manage crops (Blé, Maïs, Tournesol, Soja) and produce sensor data destined for a Kafka → MinIO → Spark → Dashboard pipeline (pipeline not yet implemented — Webots simulation is the current active component).

## Running the Simulation

Open Webots and load `worlds/agrAI.wbt`. The simulation runs automatically — no separate build step. Controllers are loaded by Webots from the `controllers/` directory.

The Python controllers require the Webots `controller` module (available only within the Webots runtime). They cannot be run standalone.

## Architecture

### Communication between controllers (IPC)

Each `field_robot` controller writes its state atomically to `/tmp/farm_field_{fid}.json` using `os.replace()` on a `.tmp` file. The `field_supervisor` controller polls these files every simulation step. This means:
- Never read a `.tmp` file — it may be incomplete.
- The supervisor reads up to 3 times on failure before skipping a field.

### field_robot state machine

Six phases in order: `sol_vide → plantation → arrosage_initial → en_croissance → mature → en_recolte`. State transitions happen only when `action_timer == 0`. The special case `recolte_done` resets to `sol_vide` after 8 steps.

Crop profiles (field IDs 1–4): Blé (1), Maïs (2), Tournesol (3), Soja (4). Each has distinct evaporation rate, nitrogen consumption, growth time, and economic values.

### field_supervisor responsibilities

- Updates soil color (via Webots field `appearance.baseColor`) based on the robot's current action.
- Updates LED color on each robot node.
- Manages progressive plant appearance (during `arrosage_initial`) and progressive harvest (during `recolte`) using independent timers.
- Plant height is computed as `height = 0.06 + (growth/100) * 0.60`; translation Z is always set to `height/2` to keep the base on the ground.

### Planned pipeline (not yet implemented)

```
Webots → Kafka (topic: sensor_data) → MinIO/raw → Spark → MinIO/clean → Dashboard
                                                      ↑
                                                  Airflow (DAGs)
```

See `PROJECT_CONTEXT.md` for the full data schema, Spark cleaning rules, Airflow DAG specs, and open questions (Kafka frequency, dashboard tool, real dataset decision).

## Key Data Schema

The JSON payload written by each robot to `/tmp/farm_field_{fid}.json` includes:
- Identity: `culture`, `fid`
- State: `action`, `phase`, `growth`
- Sensors: `hum`, `nit`, `temp`, `ph`, `stress`
- Computed: `plant_health`, `soil_fertility`
- Resources: `water_used_L`, `fertilizer_used_kg`
- Economics: `harvest_count`, `total_yield_kg`, `total_revenue`, `revenue_per_cycle`, `price_per_kg`
- Flag: `harvest_done`

## Webots Node Naming Conventions

- Fields: `FIELD_1` to `FIELD_4`
- Plants: `PLANT_{fid}_{1..9}` (9 plants per field)
- Robots: `robot_field_1` to `robot_field_4`
- Supervisor robot: `supervisor` (controller `field_supervisor`, `supervisor TRUE`)
- Each robot has a `status_led` LED device and a `color_sensor` Camera device.
