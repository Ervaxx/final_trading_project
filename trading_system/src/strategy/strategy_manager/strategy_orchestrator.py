from typing import Dict, List, Optional
from datetime import datetime
import asyncio
import logging

from src.data.providers.base_provider import DataProvider
from src.strategy.base.base_strategy import BaseStrategy
from src.execution.position.position_manager import PositionManager
from src.portfolio.tracker.portfolio_tracker import PortfolioTracker
from src.risk.risk_manager import RiskManager
from src.utils.logger import setup_logger

class StrategyOrchestrator:
    """Manages and coordinates all trading strategies"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.logger = setup_logger(__name__, config['logging'])
        
        # Initialize core components
        self.data_provider = self._init_data_provider()
        self.position_manager = self._init_position_manager()
        self.portfolio_tracker = self._init_portfolio_tracker()
        self.risk_manager = self._init_risk_manager()
        
        # Initialize strategies
        self.strategies = self._init_strategies()
        self.active = False
    
    def _init_data_provider(self) -> DataProvider:
        """Initialize market data provider"""
        provider_config = self.config['data_provider']
        return DataProvider(provider_config)
    
    def _init_position_manager(self) -> PositionManager:
        """Initialize position manager"""
        position_config = self.config['position_manager']
        return PositionManager(position_config)
    
    def _init_portfolio_tracker(self) -> PortfolioTracker:
        """Initialize portfolio tracker"""
        portfolio_config = self.config['portfolio']
        return PortfolioTracker(portfolio_config)
    
    def _init_risk_manager(self) -> RiskManager:
        """Initialize risk manager"""
        risk_config = self.config['risk']
        return RiskManager(risk_config)
    
    def _init_strategies(self) -> Dict[str, BaseStrategy]:
        """Initialize trading strategies"""
        strategies = {}
        for strat_config in self.config['strategies']:
            strategy = BaseStrategy.create(
                strat_config,
                self.data_provider,
                self.position_manager
            )
            strategies[strat_config['name']] = strategy
        return strategies
    
    async def run(self):
        """Run the trading system"""
        try:
            self.logger.info("Starting trading system...")
            self.active = True
            
            # Start core services
            await self._start_services()
            
            # Main trading loop
            while self.active:
                await self._trading_cycle()
                await asyncio.sleep(self.config['cycle_interval'])
                
        except Exception as e:
            self.logger.error(f"Trading system error: {e}")
            raise
        finally:
            await self._shutdown()
    
    async def _start_services(self):
        """Start all core services"""
        await self.data_provider.start()
        await self.position_manager.start()
        await self.portfolio_tracker.start()
        await self.risk_manager.start()
    
    async def _trading_cycle(self):
        """Execute one trading cycle"""
        try:
            # Update market data
            market_data = await self.data_provider.get_market_data()
            
            # Update portfolio status
            portfolio = await self.portfolio_tracker.get_status()
            
            # Check risk limits
            if not await self.risk_manager.check_limits(portfolio):
                self.logger.warning("Risk limits reached")
                return
            
            # Execute strategies
            tasks = []
            for strategy in self.strategies.values():
                if strategy.is_enabled():
                    tasks.append(
                        self._execute_strategy(strategy, market_data)
                    )
            
            # Run strategies concurrently
            await asyncio.gather(*tasks)
            
            # Update tracking
            await self.portfolio_tracker.update()
            
        except Exception as e:
            self.logger.error(f"Cycle error: {e}")
    
    async def _execute_strategy(self, strategy: BaseStrategy, market_data: Dict):
        """Execute single strategy"""
        try:
            # Generate signals
            signals = await strategy.generate_signals(market_data)
            
            # Validate signals
            valid_signals = await self.risk_manager.validate_signals(
                signals,
                strategy.get_positions()
            )
            
            # Execute signals
            for signal in valid_signals:
                await self.position_manager.execute_signal(signal)
                
        except Exception as e:
            self.logger.error(f"Strategy execution error: {e}")
    
    async def _shutdown(self):
        """Shutdown the trading system"""
        self.logger.info("Shutting down trading system...")
        self.active = False
        
        # Close all positions if configured
        if self.config.get('close_positions_on_shutdown', True):
            await self.position_manager.close_all_positions()
        
        # Stop all services
        await self.data_provider.stop()
        await self.position_manager.stop()
        await self.portfolio_tracker.stop()
        await self.risk_manager.stop()