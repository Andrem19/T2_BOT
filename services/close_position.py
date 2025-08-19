from exchanges.bybit_option_hub import update_leg_subscriptions
from exchanges.bybit_option_hub import BybitOptionHub as BB
from exchanges.hyperliquid_api import HL
from database.hist_trades import save_trade
from helpers.safe_sender import safe_send
import uuid
import shared_vars as sv
import asyncio
from shared_vars import logger


async def close_position_fully(pos_name, reason, last_px):
    try:
        await save_trade(pos_name, last_px, reason)
    except Exception as e:
        logger.exception(f'{e}')
    
    acc = 2 if pos_name =='first' else 1
    symbol_fut = sv.stages[pos_name]['base_coin'] + 'USDT'
    HL.cancel_all_orders(symbol_fut, acc)
    if not 'no_fut' in reason:
        position = HL.get_position(symbol_fut, acc)
        try:
            fut_size = float(position.get("size", 0) or 0)
        except Exception:  # noqa: BLE001
            fut_size = 0.0
        logger.info(f'in close_position_fully: fut_size: {fut_size}')
        if abs(fut_size) >= 1e-9:
            await close_futures(acc, symbol_fut, pos_name, fut_size)
    
    if reason != 'no_opt':
        await close_option(acc, reason, pos_name)
    
    # обнулить состояние
    sv.stages[pos_name]['exist'] = False
    sv.stages[pos_name]['position']['exist'] = False
    sv.stages[pos_name]['position']['leg']['exist'] = False
    
    try:
        await safe_send("TELEGRAM_API", f"{pos_name} closed ({reason}).", "", False)
    except Exception:
        logger.exception("%s ERROR safe_send in close_position_fully.", pos_name)
    
    update_leg_subscriptions(sv.stages)
    


async def close_futures(acc, symbol_fut, pos_name, fut_size):
    try:
        side_fx = "Buy" if fut_size > 0 else 'Sell'
        amt_base = abs(fut_size)
        tx_sig, px_exec = HL.open_market_order(
            coin=symbol_fut,
            sd=side_fx,
            amount_usdt=0.0,
            reduce_only=True,
            amount_coins=amt_base,
            account_idx=acc
        )
        await asyncio.sleep(1)
        
        logger.info(
            "%s CLOSE FUT %s pos_name=%s qty=%s side=%s px_exec≈%s",
            pos_name, symbol_fut, pos_name, amt_base, side_fx, px_exec,
        )
        return {"tx_sig": tx_sig, "px": px_exec}
    except Exception as e:  # noqa: BLE001
        logger.exception("%s ERROR close_futures %s: %s", pos_name, symbol_fut, e)
        return None


async def close_option(acc, reason, pos_name):
    try:
        symbol = sv.stages[pos_name]['position']['leg']['name']
        size = sv.stages[pos_name]['position']['leg']['info']['size']
        side = "Sell"
        
        snapshot = BB.Chain.get_snapshot(symbol=symbol, testnet=False, with_greeks=False)
        
        bidPrice = snapshot['bidPrice']
        bidSize = snapshot['bidSize']
        logger.info(f'Closing option: bidPrice: {bidPrice}, bidSize: {bidSize}')
        if not bidPrice or bidPrice <= 0:
            logger.warning("%s Cannot flatten %s: no valid price bid=%s.", pos_name, symbol, bidPrice)
            return None
        
        tp_id = sv.stages[pos_name]['position']['leg'].get("tp_id")
        if tp_id:
            try:
                BB.Trading.cancel_all_orders(account_idx=acc)
                logger.info("%s Cancelled option TP %s (%s).", pos_name, symbol, tp_id)
            except Exception:
                logger.exception("%s ERROR cancelling TP %s (%s).", pos_name, symbol, tp_id)
                
        open_options = BB.Trading.get_open_positions(account_idx=acc)
        if len(open_options)>0:
            try:
                link_id = f"monflat-{uuid.uuid4().hex[:10]}"
                resp = BB.Trading.place_limit_order(
                    account_idx=acc,
                    symbol=symbol,
                    side=side,
                    price=bidPrice,
                    qty=size,
                    time_in_force="IOC",
                    order_link_id=link_id,
                )
                closed = False
                for _ in range(4):
                    open_options = BB.Trading.get_open_positions(account_idx=acc)
                    if not open_options:
                        closed = True
                        break
                    await asyncio.sleep(5)
                
                if not closed:
                    await safe_send("TELEGRAM_API", f"{pos_name} can't closed ({reason}).", "", False)
                
                logger.info("%s CLOSE OPTION %s side=%s qty=%s px=%s (%s)", pos_name, symbol, side, size, bidPrice, reason)
            
            except Exception as e:
                logger.exception("%s ERROR flatten option %s: %s", pos_name, symbol, e)
                resp = None
            if resp:
                sv.stages[pos_name]['position']['leg']['exist'] = False
        
        return resp
            
    except Exception as e:
        logger.exception("%s ERROR close_option %s: %s", pos_name, symbol, e)
        return None