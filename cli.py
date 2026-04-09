#!/usr/bin/env python3
"""
QuantsLab CLI - Main entry point for task management
"""
import asyncio
import argparse
import sys
import os
from dotenv import load_dotenv

from core.tasks.runner import TaskRunner
from core.tasks.base import TaskConfig
import logging

# Load environment variables from .env file
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


async def clear_stale_locks():
    """Clear all is_running flags in task_schedules on startup.

    A fresh process cannot have running tasks, so any is_running=True
    left over from a previous crash is stale.  Without this, the
    orchestrator permanently refuses to schedule the locked tasks.
    """
    from pymongo import MongoClient

    mongo_uri = os.getenv("MONGO_URI")
    mongo_db = os.getenv("MONGO_DATABASE", "quants_lab")
    if not mongo_uri:
        return

    client = MongoClient(mongo_uri)
    db = client[mongo_db]
    result = db.task_schedules.update_many(
        {"is_running": True},
        {"$set": {"is_running": False, "current_execution_id": None}},
    )
    if result.modified_count:
        logger.warning(
            f"Startup: cleared {result.modified_count} stale task lock(s) "
            f"from previous run"
        )
    client.close()


def parse_args():
    parser = argparse.ArgumentParser(
        description='QuantsLab Task Management CLI',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run tasks continuously from templates
  python cli.py run-tasks --config template_1_candles_optimization.yml
  python cli.py run-tasks --config template_2_candles_pools_screener.yml
  python cli.py run-tasks --config template_3_periodic_reports.yml
  python cli.py run-tasks --config template_4_notebook_execution.yml
  
  # Run single task from config
  python cli.py trigger-task --task candles_downloader --config template_1_candles_optimization.yml
  python cli.py trigger-task --task market_screener --config template_2_candles_pools_screener.yml
  
  # List available tasks
  python cli.py list-tasks --config template_1_candles_optimization.yml
  
  # Validate configuration
  python cli.py validate-config --config template_3_periodic_reports.yml
  
  # Start API server with tasks
  python cli.py serve --config template_2_candles_pools_screener.yml --port 8000
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Run tasks continuously
    run_parser = subparsers.add_parser('run-tasks', help='Run tasks continuously')
    run_parser.add_argument('--config', '-c', 
                           default='template_1_candles_optimization.yml',
                           help='Configuration file name (from config/ directory)')
    run_parser.add_argument('--verbose', '-v', action='store_true',
                           help='Enable verbose logging')
    
    # Trigger single task
    trigger_parser = subparsers.add_parser('trigger-task', help='Run a single task once')
    trigger_parser.add_argument('--task', '-t', required=True,
                               help='Task name to trigger')
    trigger_parser.add_argument('--config', '-c',
                               default='template_1_candles_optimization.yml', 
                               help='Configuration file name (from config/ directory)')
    trigger_parser.add_argument('--timeout', type=int, default=300,
                               help='Task timeout in seconds')
    
    # Run task directly with built-in defaults
    direct_parser = subparsers.add_parser('run', help='Run a task directly with built-in defaults')
    direct_parser.add_argument('task_path', 
                              help='Task module path (e.g., app.tasks.data_collection.pools_screener)')
    direct_parser.add_argument('--timeout', type=int, default=600,
                              help='Task timeout in seconds')
    
    # Serve API with tasks
    serve_parser = subparsers.add_parser('serve', help='Start API server with background tasks')
    serve_parser.add_argument('--config', '-c',
                             default='template_1_candles_optimization.yml',
                             help='Configuration file name (from config/ directory)')
    serve_parser.add_argument('--port', '-p', type=int, default=8000,
                             help='API server port')
    serve_parser.add_argument('--host', default='0.0.0.0',
                             help='API server host')
    
    # List tasks
    list_parser = subparsers.add_parser('list-tasks', help='List available tasks')
    list_parser.add_argument('--config', '-c',
                            default='template_1_candles_optimization.yml',
                            help='Configuration file name (from config/ directory)')
    
    # Validate config
    validate_parser = subparsers.add_parser('validate-config', help='Validate task configuration')
    validate_parser.add_argument('--config', '-c', required=True,
                                help='Configuration file name (from config/ directory)')

    # Scaffold a new strategy
    scaffold_parser = subparsers.add_parser('scaffold-strategy', help='Generate files for a new trading strategy')
    scaffold_parser.add_argument('--name', '-n', required=True,
                                help='Engine name (e.g. E3)')
    scaffold_parser.add_argument('--display', '-d', required=True,
                                help='Display name (e.g. "Mean Reversion RSI")')

    # Research process tracking
    research_parser = subparsers.add_parser('research', help='Research process tracking')
    research_sub = research_parser.add_subparsers(dest='research_command')

    rs_status = research_sub.add_parser('status', help='Show research phase for an engine')
    rs_status.add_argument('--engine', '-e', required=True)

    rs_advance = research_sub.add_parser('advance', help='Record completion of a research phase')
    rs_advance.add_argument('--engine', '-e', required=True)
    rs_advance.add_argument('--phase', '-p', required=True,
                           help='Phase name (e.g. P1_REGIME, P2_MONTECARLO)')
    rs_advance.add_argument('--notes', default='', help='Free-text notes')

    rs_all = research_sub.add_parser('all', help='Show research status for all engines')

    return parser.parse_args()


async def run_tasks(config_path: str, verbose: bool = False):
    """Run tasks continuously."""
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Add config/ prefix if not present
    if not config_path.startswith('config/') and not os.path.isabs(config_path):
        config_path = f'config/{config_path}'

    logger.info(f"Starting QuantsLab Task Runner v2.0")
    logger.info(f"Config: {config_path}")

    # Clear stale locks from any previous crashed run
    await clear_stale_locks()

    try:
        # Run tasks without API server (API disabled by default)
        runner = TaskRunner(config_path=config_path, enable_api=False)
        await runner.start()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as e:
        logger.error(f"Error running tasks: {e}")
        sys.exit(1)


async def trigger_task(task_name: str, config_path: str, timeout: int):
    """Trigger a single task."""
    # Add config/ prefix if not present
    if not config_path.startswith('config/') and not os.path.isabs(config_path):
        config_path = f'config/{config_path}'

    logger.info(f"Triggering task: {task_name}")
    logger.info(f"Config: {config_path}")
    logger.info(f"Timeout: {timeout}s")

    # Clear stale locks from any previous crashed run
    await clear_stale_locks()

    try:
        runner = TaskRunner(config_path=config_path)

        # Setup storage and orchestrator
        from core.tasks.storage import MongoDBTaskStorage
        storage = MongoDBTaskStorage()
        await storage.initialize()

        from core.tasks.orchestrator import TaskOrchestrator
        max_concurrent = runner.config.get("max_concurrent_tasks", 10)
        runner.orchestrator = TaskOrchestrator(
            storage=storage,
            max_concurrent_tasks=max_concurrent,
            retry_failed_tasks=runner.config.get("retry_failed_tasks", True)
        )
        
        # Initialize tasks
        tasks = await runner._initialize_tasks()
        for task in tasks:
            runner.orchestrator.add_task(task)
        
        # Trigger specific task
        result = await runner.orchestrator.execute_task(
            task_name=task_name,
            force=True
        )
        
        if result:
            logger.info(f"Task {task_name} completed with status: {result.status}")
            if result.error_message:
                logger.error(f"Error: {result.error_message}")
                sys.exit(1)
        else:
            logger.error(f"Task {task_name} not found or could not be executed")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Error triggering task: {e}")
        sys.exit(1)


async def serve_api(config_path: str, host: str, port: int):
    """Start API server with background tasks."""
    # Add config/ prefix if not present
    if not config_path.startswith('config/') and not os.path.isabs(config_path):
        config_path = f'config/{config_path}'

    logger.info(f"Starting QuantsLab API Server")
    logger.info(f"Config: {config_path}")
    logger.info(f"Server: http://{host}:{port}")

    # Clear stale locks from any previous crashed run
    await clear_stale_locks()

    try:
        # Create runner with API enabled and configure host/port
        runner = TaskRunner(config_path=config_path, enable_api=True)
        runner.api_host = host
        runner.api_port = port
        await runner.start()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as e:
        logger.error(f"Error running server: {e}")
        sys.exit(1)


async def run_task_direct(task_path: str, timeout: int):
    """Run a task directly using its built-in main() function."""
    logger.info(f"Running task directly: {task_path}")
    logger.info(f"Timeout: {timeout}s")
    
    try:
        # Import the task module
        import importlib
        module = importlib.import_module(task_path)
        
        if not hasattr(module, 'main'):
            logger.error(f"Task module {task_path} does not have a main() function")
            sys.exit(1)
        
        # Execute with timeout
        await asyncio.wait_for(module.main(), timeout=timeout)
        logger.info(f"Task {task_path} completed successfully")
        
    except asyncio.TimeoutError:
        logger.error(f"Task {task_path} timed out after {timeout} seconds")
        sys.exit(1)
    except ImportError as e:
        logger.error(f"Failed to import task {task_path}: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error running task {task_path}: {e}")
        sys.exit(1)


def list_tasks(config_path: str):
    """List available tasks from configuration."""
    # Add config/ prefix if not present
    if not config_path.startswith('config/') and not os.path.isabs(config_path):
        config_path = f'config/{config_path}'
    
    logger.info(f"Loading tasks from: {config_path}")
    
    try:
        if not os.path.exists(config_path):
            logger.error(f"Config file not found: {config_path}")
            sys.exit(1)
        
        runner = TaskRunner(config_path=config_path)
        tasks_config = runner.load_config()
        
        print("\nAvailable Tasks:")
        print("=" * 50)
        for task_name, task_config in tasks_config.get('tasks', {}).items():
            enabled = task_config.get('enabled', True)
            status = "✓ enabled" if enabled else "✗ disabled" 
            task_class = task_config.get('task_class', 'Unknown')
            schedule = task_config.get('schedule', {})
            schedule_info = f"({schedule.get('type', 'unknown')})"
            print(f"{task_name:30} {status:12} {task_class} {schedule_info}")
    
    except Exception as e:
        logger.error(f"Error listing tasks: {e}")
        sys.exit(1)


def validate_config(config_path: str):
    """Validate task configuration file."""
    # Add config/ prefix if not present
    if not config_path.startswith('config/') and not os.path.isabs(config_path):
        config_path = f'config/{config_path}'
    
    logger.info(f"Validating config: {config_path}")
    
    try:
        if not os.path.exists(config_path):
            logger.error(f"Config file not found: {config_path}")
            sys.exit(1)
        
        runner = TaskRunner(config_path=config_path)
        tasks_config = runner.load_config()
        
        # Validate each task config
        errors = []
        for task_name, task_config in tasks_config.get('tasks', {}).items():
            try:
                TaskConfig(**task_config, name=task_name)
            except Exception as e:
                errors.append(f"Task {task_name}: {e}")
        
        if errors:
            logger.error("Validation errors found:")
            for error in errors:
                logger.error(f"  - {error}")
            sys.exit(1)
        else:
            logger.info("✓ Config is valid")
            
    except Exception as e:
        logger.error(f"✗ Config validation failed: {e}")
        sys.exit(1)


def scaffold_strategy(name: str, display_name: str):
    """Generate template files for a new trading strategy."""
    import re

    # Derive file names
    snake = re.sub(r'[^a-z0-9]+', '_', display_name.lower()).strip('_')
    engine_file = f"app/engines/{name.lower()}_{snake}.py"
    controller_file = f"app/controllers/directional_trading/{name.lower()}_{snake}.py"

    # Check nothing exists already
    for path in [engine_file, controller_file]:
        if os.path.exists(path):
            logger.error(f"File already exists: {path}")
            sys.exit(1)

    # Engine evaluation function template
    engine_content = f'''"""
Engine {name} — {display_name}

TODO: Describe the strategy thesis here.
"""
from dataclasses import dataclass, field
from typing import Optional

from app.engines.models import CandidateBase, DecisionSnapshot, validate_staleness


@dataclass
class {name}Candidate(CandidateBase):
    """{name}-specific candidate fields (extends CandidateBase)."""
    # TODO: Add engine-specific fields here
    pass


def evaluate_{name.lower()}(snap: DecisionSnapshot) -> {name}Candidate:
    """
    {name} evaluation pipeline. Reads ONLY from snapshot.

    TODO: Implement your strategy logic here.
    """
    cand = {name}Candidate(
        snapshot_id=snap.snapshot_id,
        pair=snap.pair,
        market_state=snap.market_state,
    )

    fr = snap.features
    if fr is None:
        cand.disposition = "FILTERED_STALENESS"
        cand.filter_reason = "No feature data in snapshot"
        return cand

    # Validate staleness
    stale_ok, stale_flags = validate_staleness(snap)
    cand.feature_staleness_flags = stale_flags
    if not stale_ok:
        cand.disposition = "FILTERED_STALENESS"
        cand.filter_reason = f"Stale: {{stale_flags}}"
        return cand

    # TODO: Implement trigger logic
    cand.trigger_fired = False
    cand.disposition = "SKIPPED_NO_TRIGGER"
    cand.trigger_reason = "Not implemented yet"
    return cand
'''

    # Controller template
    controller_content = f'''"""
{name} {display_name} — HB V2 Controller for backtesting.

TODO: Implement the controller for backtesting this strategy.
"""
from decimal import Decimal
from typing import List, Optional

from hummingbot.strategy_v2.controllers.directional_trading_controller_base import (
    DirectionalTradingControllerBase,
    DirectionalTradingControllerConfigBase,
)


class {name}{snake.title().replace("_", "")}Config(DirectionalTradingControllerConfigBase):
    """Configuration for {name} {display_name} controller."""
    controller_name: str = "{name.lower()}_{snake}"
    # TODO: Add strategy-specific config fields


class {name}{snake.title().replace("_", "")}Controller(DirectionalTradingControllerBase):
    """Backtesting controller for {name} {display_name}."""

    def __init__(self, config: {name}{snake.title().replace("_", "")}Config, *args, **kwargs):
        super().__init__(config, *args, **kwargs)
        self.config = config

    async def update_processed_data(self):
        """Process candle data and compute indicators."""
        # TODO: Implement signal computation on self.candles_df
        pass
'''

    # Write files
    os.makedirs(os.path.dirname(engine_file), exist_ok=True)
    os.makedirs(os.path.dirname(controller_file), exist_ok=True)

    with open(engine_file, 'w') as f:
        f.write(engine_content)
    with open(controller_file, 'w') as f:
        f.write(controller_content)

    print(f"\\nStrategy '{name} — {display_name}' scaffolded:\\n")
    print(f"  Engine:     {engine_file}")
    print(f"  Controller: {controller_file}")
    print(f"\\nResearch process (enforced — engine blocked from trading until Phase 2 complete):")
    print(f"  P0_IDEA         Define hypothesis + success criteria")
    print(f"  P1_BASELINE     Walk-forward backtest")
    print(f"  P1_REGIME       Regime stress tests (bear/shock/range/bull)")
    print(f"  P1_ASYMMETRY    Long/short analysis + direction decision")
    print(f"  P1_SENSITIVITY  Parameter sensitivity (+/- 1 step)")
    print(f"  P1_OPTIMIZED    Optuna optimization (optional)")
    print(f"  P1_VALIDATED    One-shot validation on holdout")
    print(f"  P2_DEGRADATION  Slippage/delay/distribution stress tests")
    print(f"  P2_MONTECARLO   Block bootstrap, ruin < 1%")
    print(f"  P3_PAPER        Paper trading (20+ signals, slippage < 15bps)")
    print(f"\\nTrack progress: python cli.py research status --engine {name}")
    print(f"Advance phase:  python cli.py research advance --engine {name} --phase P0_IDEA")
    print(f"\\nCode steps:")
    print(f"  1. Implement evaluate_{name.lower()}() in {engine_file}")
    print(f"  2. Implement controller in {controller_file}")
    print(f"  3. Add entry to STRATEGY_REGISTRY in app/engines/strategy_registry.py")
    print(f"  4. Add '{name}' to engines list in config/hermes_pipeline.yml signal_scan")


async def research_command(args):
    """Handle research process tracking commands."""
    from pymongo import MongoClient
    from app.engines.research_tracker import ResearchTracker, ResearchPhase, PHASE_REQUIREMENTS

    mongo_uri = os.getenv("MONGO_URI")
    if not mongo_uri:
        logger.error("MONGO_URI not set")
        sys.exit(1)

    client = MongoClient(mongo_uri)
    db = client[os.getenv("MONGO_DATABASE", "quants_lab")]
    tracker = ResearchTracker(db)

    if args.research_command == 'status':
        status = await tracker.get_status_summary(args.engine)
        print(f"\n{status}")

        # Show requirements for next phase
        phase = await tracker.get_phase(args.engine)
        next_val = phase.value + 10
        try:
            next_phase = ResearchPhase(next_val)
            reqs = PHASE_REQUIREMENTS.get(next_phase, [])
            if reqs:
                print(f"\n  Next phase ({next_phase.name}) requires:")
                for r in reqs:
                    print(f"    - {r}")
        except ValueError:
            pass

    elif args.research_command == 'advance':
        # For CLI advancement, collect evidence interactively
        phase_name = args.phase.upper()
        try:
            phase_enum = ResearchPhase[phase_name]
        except KeyError:
            logger.error(f"Unknown phase: {phase_name}")
            print(f"Valid phases: {[p.name for p in ResearchPhase]}")
            sys.exit(1)

        reqs = PHASE_REQUIREMENTS.get(phase_enum, [])
        print(f"\nAdvancing {args.engine} to {phase_name}")
        print(f"Required evidence: {reqs}")
        print(f"(Pass evidence via code or notebook — CLI records the advance)")

        # Minimal evidence for CLI-driven advance
        evidence = {"advanced_via": "cli", "notes": args.notes}
        for r in reqs:
            evidence[r] = f"[recorded via CLI — see notes]"

        ok = await tracker.advance(args.engine, phase_name, evidence, args.notes)
        if ok:
            print(f"Advanced {args.engine} to {phase_name}")
        else:
            print(f"Failed to advance — check requirements")
            sys.exit(1)

    elif args.research_command == 'all':
        from app.engines.strategy_registry import STRATEGY_REGISTRY
        print("\n=== Research Status ===")
        for engine in STRATEGY_REGISTRY:
            status = await tracker.get_status_summary(engine)
            print(f"\n{status}")

    else:
        logger.error("Unknown research command. Use: status, advance, all")
        sys.exit(1)

    client.close()


async def main():
    args = parse_args()

    if args.command == 'run-tasks':
        await run_tasks(args.config, args.verbose)
    elif args.command == 'trigger-task':
        await trigger_task(args.task, args.config, args.timeout)
    elif args.command == 'run':
        await run_task_direct(args.task_path, args.timeout)
    elif args.command == 'serve':
        await serve_api(args.config, args.host, args.port)
    elif args.command == 'list-tasks':
        list_tasks(args.config)
    elif args.command == 'validate-config':
        validate_config(args.config)
    elif args.command == 'scaffold-strategy':
        scaffold_strategy(args.name, args.display)
    elif args.command == 'research':
        await research_command(args)
    else:
        logger.error("No command specified. Use --help for usage.")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())