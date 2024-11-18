# #!/usr/bin/env python3
# """
# Trading System Main Entry Point
# """

# import asyncio
# import logging
# from pathlib import Path
# from src.utils.config import load_config
# from src.strategy_manager import StrategyOrchestrator

# async def main():
#     """Main entry point"""
#     try:
#         # Load configuration
#         config = load_config("config/main_config.yaml")
        
#         # Initialize system
#         orchestrator = StrategyOrchestrator(config)
        
#         # Run system
#         await orchestrator.run()
        
#     except Exception as e:
#         logging.error(f"System error: {e}")
#         raise

# if __name__ == "__main__":
#     asyncio.run(main())
