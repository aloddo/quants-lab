#!/usr/bin/env python3
"""
QuantsLab CLI - Main entry point for task management
"""
import asyncio
import argparse
import sys
import os

import aiohttp
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


async def run_startup_exchange_audit():
    """Verify exchange positions match MongoDB state before trading starts.

    Detects orphaned positions left by a previous crash. Must run AFTER
    clear_stale_locks() and BEFORE the task orchestrator starts.
    """
    from pymongo import MongoClient
    from app.tasks.resolution.reconciliation import startup_exchange_audit

    mongo_uri = os.getenv("MONGO_URI")
    mongo_db = os.getenv("MONGO_DATABASE", "quants_lab")
    if not mongo_uri:
        return

    client = MongoClient(mongo_uri)
    db = client[mongo_db]

    logger.info("Startup: running exchange position audit...")
    try:
        result = await startup_exchange_audit(db)
        if result.get("skipped"):
            logger.warning("Startup: exchange audit skipped (HB API unavailable)")
        elif result.get("orphan_positions", 0) > 0 or result.get("stale_candidates", 0) > 0:
            logger.warning(
                f"Startup: exchange audit found issues — "
                f"{result.get('orphan_positions', 0)} orphan position(s), "
                f"{result.get('stale_candidates', 0)} stale candidate(s)"
            )
        else:
            logger.info(
                f"Startup: exchange audit clean — "
                f"{result.get('exchange_positions', 0)} position(s) verified"
            )
    except Exception as e:
        logger.error(f"Startup: exchange audit failed: {e}")
    finally:
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
    serve_parser.add_argument('--host', default='127.0.0.1',
                             help='API server host (default: 127.0.0.1, use 0.0.0.0 for remote access)')
    
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

    # Promote a shadow engine to live
    shadow_parser = subparsers.add_parser(
        'promote-shadow',
        help='Promote a shadow engine: show parameter diff and emit update instructions',
    )
    shadow_parser.add_argument('--engine', '-e', required=True,
                               help='Shadow engine name (e.g. E1_shadow)')
    shadow_parser.add_argument('--to', required=True,
                               help='Live engine to promote into (e.g. E1)')

    # ── Bot Orchestration ──────────────────────────────────
    deploy_parser = subparsers.add_parser('deploy', help='Deploy engine as HB-native bot')
    deploy_parser.add_argument('--engine', '-e', required=True, help='Engine name (e.g. E1)')
    deploy_parser.add_argument('--pair', '-p', help='Single pair to deploy (testing)')
    deploy_parser.add_argument('--pairs', help='Comma-separated pairs to deploy (e.g. BTC-USDT,ETH-USDT)')
    deploy_parser.add_argument('--dry-run', action='store_true', help='Show configs without deploying')
    deploy_parser.add_argument('--force', action='store_true', help='Deploy even if positions exist on exchange')
    deploy_parser.add_argument('--profile', default='master_account', help='HB credentials profile')
    deploy_parser.add_argument('--instance-suffix', default='paper', help='Bot instance name suffix (default: paper)')

    bot_status_parser = subparsers.add_parser('bot-status', help='Check HB bot status')
    bot_status_parser.add_argument('--engine', '-e', help='Engine name (omit for all bots)')

    bot_stop_parser = subparsers.add_parser('bot-stop', help='Stop HB bot')
    bot_stop_parser.add_argument('--engine', '-e', required=True, help='Engine name')

    bot_redeploy_parser = subparsers.add_parser('bot-redeploy', help='Stop, regenerate configs, redeploy')
    bot_redeploy_parser.add_argument('--engine', '-e', required=True, help='Engine name')
    bot_redeploy_parser.add_argument('--profile', default='master_account', help='HB credentials profile')

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

    # Verify exchange positions match MongoDB state
    await run_startup_exchange_audit()

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
        # Create runner with API enabled (no periodic scheduler loop).
        # Pipeline scheduling should be owned by `run-tasks` instance.
        runner = TaskRunner(config_path=config_path, enable_api=True, start_scheduler=False)
        runner.api_host = host
        runner.api_port = port

        # Inject dashboard router AFTER TaskRunner is initialised (so core.tasks.api
        # is fully imported and app is the same object uvicorn will serve).
        try:
            from core.tasks.api import app as fastapi_app
            from app.web.dashboard import router as dashboard_router
            fastapi_app.include_router(dashboard_router)
            logger.info("Dashboard mounted at /dashboard")
        except Exception as e:
            logger.warning(f"Could not mount dashboard router: {e}")

        # Optional bearer token auth (set QUANTS_LAB_API_TOKEN to enable)
        api_token = os.environ.get("QUANTS_LAB_API_TOKEN")
        if api_token:
            from starlette.middleware.base import BaseHTTPMiddleware
            from starlette.responses import JSONResponse

            class BearerAuthMiddleware(BaseHTTPMiddleware):
                async def dispatch(self, request, call_next):
                    if request.url.path in ("/health", "/docs", "/openapi.json"):
                        return await call_next(request)
                    auth = request.headers.get("authorization", "")
                    if auth != f"Bearer {api_token}":
                        return JSONResponse({"detail": "Unauthorized"}, status_code=401)
                    return await call_next(request)

            fastapi_app.add_middleware(BearerAuthMiddleware)
            logger.info("API auth enabled (QUANTS_LAB_API_TOKEN set)")

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
    """Generate a self-contained HB V2 controller for a new strategy."""
    import re

    snake = re.sub(r'[^a-z0-9]+', '_', display_name.lower()).strip('_')
    controller_file = f"app/controllers/directional_trading/{name.lower()}_{snake}.py"

    if os.path.exists(controller_file):
        logger.error(f"File already exists: {controller_file}")
        sys.exit(1)

    config_cls = f"{name}{snake.title().replace('_', '')}Config"
    ctrl_cls = f"{name}{snake.title().replace('_', '')}Controller"

    controller_content = f'''"""
{name} — {display_name} (HB V2 Controller)

Self-contained: no quants-lab imports. Same code runs in backtest and live.

Thesis: TODO describe the inefficiency and why it's exploitable.

Trigger: TODO
Exit: TODO
"""
from typing import List, Dict, Any, Optional

import pandas as pd
import pandas_ta as ta
from pydantic import Field

from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy_v2.controllers.directional_trading_controller_base import (
    DirectionalTradingControllerBase,
    DirectionalTradingControllerConfigBase,
)
from hummingbot.strategy_v2.executors.position_executor.data_types import (
    PositionExecutorConfig,
    TripleBarrierConfig,
)


class {config_cls}(DirectionalTradingControllerConfigBase):
    """Configuration for {name} {display_name}."""
    controller_name: str = "{name.lower()}_{snake}"

    # TODO: Add strategy-specific config fields
    candles_config: List[CandlesConfig] = Field(default_factory=list)

    def model_post_init(self, __context) -> None:
        if not self.candles_config:
            self.candles_config = [
                CandlesConfig(
                    connector=self.connector_name,
                    trading_pair=self.trading_pair,
                    interval="1h",
                    max_records=500,
                ),
            ]


class {ctrl_cls}(DirectionalTradingControllerBase):
    """{name} {display_name} — backtest + live controller."""

    def __init__(self, config: {config_cls}, *args, **kwargs):
        self.config = config
        super().__init__(config, *args, **kwargs)

    def get_candles_config(self) -> List[CandlesConfig]:
        return self.config.candles_config

    async def update_processed_data(self):
        """Fetch candles, compute features, emit signal."""
        c = self.config
        df = self.market_data_provider.get_candles_df(
            connector_name=c.connector_name,
            trading_pair=c.trading_pair,
            interval="1h",
            max_records=500,
        )
        if df is None or len(df) < 50:
            self.processed_data["signal"] = 0
            self.processed_data["signal_reason"] = "insufficient_data"
            return

        # TODO: compute indicators and set signal
        # self.processed_data["signal"] = 1 for LONG, -1 for SHORT, 0 for no signal
        self.processed_data["signal"] = 0
        self.processed_data["signal_reason"] = "not_implemented"
'''

    os.makedirs(os.path.dirname(controller_file), exist_ok=True)
    with open(controller_file, 'w') as f:
        f.write(controller_content)

    print(f"\nStrategy '{name} — {display_name}' scaffolded:\n")
    print(f"  Controller: {controller_file}")
    print(f"\nProcess (HB-native):")
    print(f"  1. Implement update_processed_data() in the controller")
    print(f"  2. Add StrategyMetadata to STRATEGY_REGISTRY (deployment_mode='hb_native')")
    print(f"  3. Backtest: python cli.py trigger-task --task {name.lower()}_bulk_backtest")
    print(f"  4. Walk-forward: python cli.py trigger-task --task {name.lower()}_walk_forward")
    print(f"  5. Deploy: python cli.py deploy --engine {name}")
    print(f"\nResearch tracking:")
    print(f"  python cli.py research status --engine {name}")
    print(f"  python cli.py research advance --engine {name} --phase P0_IDEA")


def promote_shadow(shadow_name: str, live_name: str):
    """
    Show parameter diff between a shadow engine and its live counterpart.
    Emits the exact code change needed to promote the shadow to live.

    Does NOT auto-modify strategy_registry.py — human review is required.
    """
    from app.engines.strategy_registry import STRATEGY_REGISTRY

    if shadow_name not in STRATEGY_REGISTRY:
        print(f"Error: '{shadow_name}' not found in STRATEGY_REGISTRY.")
        print(f"Registered engines: {list(STRATEGY_REGISTRY.keys())}")
        sys.exit(1)

    if live_name not in STRATEGY_REGISTRY:
        print(f"Error: '{live_name}' not found in STRATEGY_REGISTRY.")
        sys.exit(1)

    shadow = STRATEGY_REGISTRY[shadow_name]
    live = STRATEGY_REGISTRY[live_name]

    if shadow.shadow_of != live_name:
        print(f"Warning: {shadow_name}.shadow_of = '{shadow.shadow_of}', expected '{live_name}'")

    print(f"\nPromote {shadow_name} → {live_name}\n{'='*50}\n")

    # Show parameter diff
    diffs = []
    for attr in ("exit_params", "trailing_stop", "direction", "blocked_pairs",
                 "required_features", "max_concurrent"):
        s_val = getattr(shadow, attr, None)
        l_val = getattr(live, attr, None)
        if s_val != l_val:
            diffs.append((attr, l_val, s_val))

    if not diffs:
        print("No parameter differences found — shadow is identical to live engine.")
        print(f"\nTo remove the shadow, delete the '{shadow_name}' entry from STRATEGY_REGISTRY.")
        return

    print("Parameter changes:\n")
    for attr, live_val, shadow_val in diffs:
        print(f"  {attr}:")
        print(f"    live:   {live_val}")
        print(f"    shadow: {shadow_val}")
        print()

    print("\nTo promote, apply these changes to app/engines/strategy_registry.py:\n")
    print(f"  In the '{live_name}' StrategyMetadata entry:")
    for attr, _, shadow_val in diffs:
        print(f"    {attr} = {repr(shadow_val)},")

    print(f"\nThen remove the '{shadow_name}' entry from STRATEGY_REGISTRY.")
    print("\nRestart pipeline after changes: sudo launchctl stop com.quantslab.pipeline")


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


BOTS_DIR = os.path.expanduser("~/hummingbot/hummingbot-api/bots")

# Pairs that are tradeable via HB API (must match rate limiter patch).
TRADEABLE_PAIRS: set = {
    "BTC-USDT", "ETH-USDT", "SOL-USDT", "XRP-USDT", "DOGE-USDT",
    "ADA-USDT", "AVAX-USDT", "LINK-USDT", "DOT-USDT", "UNI-USDT",
    "NEAR-USDT", "APT-USDT", "ARB-USDT", "OP-USDT", "SUI-USDT",
    "SEI-USDT", "WLD-USDT", "LTC-USDT", "BCH-USDT", "BNB-USDT",
    "CRV-USDT", "1000PEPE-USDT", "ALGO-USDT", "GALA-USDT",
    "ONT-USDT", "TAO-USDT", "ZEC-USDT",
    "AAVE-USDT", "DRIFT-USDT", "FARTCOIN-USDT", "PAXG-USDT", "WIF-USDT",
}


async def deploy_engine(engine_name: str, single_pair: str = None,
                        dry_run: bool = False, profile: str = "master_account",
                        force: bool = False, pairs_csv: str = None,
                        instance_suffix: str = "paper"):
    """Deploy an engine as an HB-native bot."""
    from pymongo import MongoClient
    from app.engines.strategy_registry import get_strategy
    from app.services.hb_api_client import HBApiClient
    import shutil

    meta = get_strategy(engine_name)
    if meta.deployment_mode != "hb_native":
        logger.error(f"{engine_name} deployment_mode is '{meta.deployment_mode}', not 'hb_native'")
        sys.exit(1)

    if not meta.controller_file:
        logger.error(f"{engine_name} has no controller_file set in registry")
        sys.exit(1)

    # 1. Get eligible pairs
    if single_pair:
        pairs = [single_pair]
    elif pairs_csv:
        pairs = [p.strip() for p in pairs_csv.split(",") if p.strip()]
    else:
        mongo_uri = os.getenv("MONGO_URI")
        if not mongo_uri:
            logger.error("MONGO_URI not set")
            sys.exit(1)
        client = MongoClient(mongo_uri)
        db = client[os.getenv("MONGO_DATABASE", "quants_lab")]

        if meta.pair_source == "pair_historical":
            docs = list(db.pair_historical.find(
                {"engine": engine_name, "verdict": "ALLOW"}, {"pair": 1}
            ))
            pairs = [d["pair"] for d in docs]
        else:
            pairs = meta.pair_allowlist

        client.close()

    # Filter by tradeable + blocked
    pairs = [p for p in pairs if p in TRADEABLE_PAIRS and p not in meta.blocked_pairs]

    if not pairs:
        logger.error(f"No eligible pairs for {engine_name}")
        sys.exit(1)

    logger.info(f"Deploying {engine_name} on {len(pairs)} pairs")

    # 1b. PRE-DEPLOY SAFETY: check exchange for existing positions
    try:
        from app.services.bybit_exchange_client import BybitExchangeClient
        exchange = BybitExchangeClient()
        if exchange.is_configured():
            async with aiohttp.ClientSession() as _session:
                existing = await exchange.fetch_positions(_session)
            existing_pairs = {p["pair"] for p in existing if p["qty"] > 0}
            overlap = set(pairs) & existing_pairs
            if overlap:
                logger.warning(
                    f"POSITION COLLISION: {len(overlap)} pair(s) already have "
                    f"open positions on the exchange:"
                )
                for p in existing:
                    if p["pair"] in overlap:
                        logger.warning(
                            f"  {p['pair']:12} {p['side']:5} qty={p['qty']:>10.4f} "
                            f"entry={p['entry_price']:.5f} uPnL=${p['unrealised_pnl']:.2f}"
                        )
                if not force:
                    logger.error(
                        "Aborting to prevent doubled positions. "
                        "Close existing positions first, or use --force to override."
                    )
                    sys.exit(1)
                else:
                    logger.warning("--force flag set, deploying despite existing positions")
    except Exception as e:
        logger.warning(f"Pre-deploy position check failed: {e} — proceeding with deploy")

    # 2. Generate controller configs
    config_names = []
    configs = {}
    for pair in pairs:
        slug = pair.lower().replace("-", "_")
        config_name = f"{engine_name.lower()}_{slug}"
        config_names.append(config_name)

        config = {
            "id": config_name,
            "controller_name": meta.controller_module.split(".")[-1],
            "controller_type": "directional_trading",
            "connector_name": meta.hb_connector,
            "trading_pair": pair,
            "total_amount_quote": meta.total_amount_quote,
            "max_executors_per_side": 1,
            "cooldown_time": meta.cooldown_time,
            "leverage": 1,
            "position_mode": "ONEWAY",
            "stop_loss": str(meta.exit_params.get("stop_loss", "0.015")),
            "take_profit": str(meta.exit_params.get("take_profit", "0.03")),
            "time_limit": meta.exit_params.get("time_limit", 86400),
        }

        if meta.trailing_stop:
            config["trailing_stop"] = {
                "activation_price": str(meta.trailing_stop["activation_price"]),
                "trailing_delta": str(meta.trailing_stop["trailing_delta"]),
            }

        # Merge strategy-specific defaults
        config.update(meta.default_config)

        # Inject API keys from env for controllers that need them in Docker
        coinalyze_key = os.getenv("COINALYZE_API_KEY", "")
        if coinalyze_key and "derivatives" in (meta.required_features or []):
            config["coinalyze_api_key"] = coinalyze_key

        configs[config_name] = config

    if dry_run:
        import json
        print(f"\n=== Dry Run: {engine_name} on {len(pairs)} pairs ===\n")
        print(f"Bot instance: {engine_name.lower()}_paper")
        print(f"Connector: {meta.hb_connector}")
        print(f"Controller: {meta.controller_file}")
        print(f"Pairs: {', '.join(pairs)}")
        print(f"\nSample config ({config_names[0]}):")
        print(json.dumps(configs[config_names[0]], indent=2))
        return

    # 3. Sync controller file to bots directory
    src = f"app/controllers/directional_trading/{meta.controller_file}"
    dst = f"{BOTS_DIR}/controllers/directional_trading/{meta.controller_file}"
    if os.path.exists(src):
        shutil.copy2(src, dst)
        logger.info(f"Synced controller: {src} → {dst}")
    else:
        logger.error(f"Controller file not found: {src}")
        sys.exit(1)

    # 4. Upload configs and deploy via HB API
    hb = HBApiClient()
    try:
        if not await hb.health_check():
            logger.error("HB API unreachable")
            sys.exit(1)

        # Controller is already synced to bots/ via filesystem (volume mount).
        # The API upload is only needed if bots/ is NOT volume-mounted.
        try:
            with open(src) as f:
                source_code = f.read()
            controller_name = meta.controller_file.replace(".py", "")
            await hb.upload_controller("directional_trading", controller_name, source_code)
            logger.info(f"Uploaded controller via API: {controller_name}")
        except Exception as e:
            logger.info(f"API upload skipped (file synced via volume mount): {e}")

        # Upload configs
        for name, config in configs.items():
            await hb.create_controller_config(name, config)
        logger.info(f"Uploaded {len(configs)} controller configs")

        # Add demo credentials if needed
        # Support multiple API keys: BYBIT_DEMO_API_KEY (default) and BYBIT_DEMO_API_KEY_2, etc.
        # The profile name determines which key to use: master_account=key1, account_b=key2
        if profile == "account_b":
            env_key = os.getenv("BYBIT_DEMO_API_KEY_2")
            env_secret = os.getenv("BYBIT_DEMO_API_SECRET_2")
        else:
            env_key = os.getenv("BYBIT_DEMO_API_KEY")
            env_secret = os.getenv("BYBIT_DEMO_API_SECRET")

        connector_name = meta.hb_connector
        if env_key and "bybit_perpetual" in connector_name:
            try:
                await hb.add_credential(profile, connector_name, {
                    f"{connector_name}_api_key": env_key,
                    f"{connector_name}_secret_key": env_secret,
                })
                logger.info(f"Added {connector_name} credentials for profile {profile}")
            except Exception as e:
                logger.warning(f"Credential add: {e} (may already exist)")

        # Deploy bot
        instance_name = f"{engine_name.lower()}_{instance_suffix}"
        result = await hb.deploy_v2_bot(
            instance_name=instance_name,
            credentials_profile=profile,
            controllers_config=config_names,
            max_controller_drawdown_quote=meta.max_drawdown_quote,
            image=meta.bot_image,
            headless=True,
        )
        logger.info(f"Deployed bot: {instance_name}")
        logger.info(f"Result: {result}")

        # Start bot — use the unique instance name returned by deploy
        actual_name = result.get("unique_instance_name", instance_name)
        start_result = await hb.start_bot(actual_name)
        logger.info(f"Started bot {actual_name}: {start_result}")

        print(f"\n{engine_name} deployed successfully!")
        print(f"  Bot: {instance_name}")
        print(f"  Pairs: {len(pairs)}")
        print(f"  Check status: python cli.py bot-status --engine {engine_name}")

    finally:
        await hb.close()


async def bot_status(engine_name: str = None):
    """Check HB bot status."""
    from app.services.hb_api_client import HBApiClient

    hb = HBApiClient()
    try:
        if not await hb.health_check():
            logger.error("HB API unreachable")
            sys.exit(1)

        if engine_name:
            instance_name = f"{engine_name.lower()}_paper"
            status = await hb.get_bot_status(instance_name)
        else:
            status = await hb.get_bot_status()

        import json
        print(json.dumps(status, indent=2, default=str))
    except Exception as e:
        logger.error(f"Bot status error: {e}")
    finally:
        await hb.close()


async def bot_stop(engine_name: str):
    """Stop an HB bot."""
    from app.services.hb_api_client import HBApiClient

    hb = HBApiClient()
    try:
        instance_name = f"{engine_name.lower()}_paper"
        result = await hb.stop_bot(instance_name)
        logger.info(f"Stopped bot: {instance_name}")
        print(f"Result: {result}")
    except Exception as e:
        logger.error(f"Bot stop error: {e}")
    finally:
        await hb.close()


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
    elif args.command == 'promote-shadow':
        promote_shadow(args.engine, getattr(args, 'to'))
    elif args.command == 'deploy':
        await deploy_engine(args.engine, args.pair, args.dry_run, args.profile,
                            getattr(args, 'force', False),
                            pairs_csv=getattr(args, 'pairs', None),
                            instance_suffix=getattr(args, 'instance_suffix', 'paper'))
    elif args.command == 'bot-status':
        await bot_status(getattr(args, 'engine', None))
    elif args.command == 'bot-stop':
        await bot_stop(args.engine)
    elif args.command == 'bot-redeploy':
        await bot_stop(args.engine)
        await deploy_engine(args.engine, profile=args.profile)
    else:
        logger.error("No command specified. Use --help for usage.")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
