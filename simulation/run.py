import simulation.get_signal as get_signal
import shared_vars as sv
import simulation.engine_2 as engine_2
import traceback


def run_logic(data, fut_calc, params):
    start_idx    = 1800
    end_idx      = len(data) - 5000
    quarter = end_idx//4

    i = start_idx
    while i < end_idx:
        try:
            # получаем сигнал
            settings = get_signal.signal(data, i)
            if not settings['decision']:
                # если без действия — считаем это за одну итерацию
                i += 1
                continue
            
            # иначе симулируем сделку
            result, it = engine_2.simulation(data, i, fut_calc, params, end_idx)
            if result is None:
                return
            sv.data_list.append(result)
            
            if i > quarter and sv.sum < 0:
                return
            
            i = it + 1

        except Exception as e:
            print("Ошибка в run_logic:", e)
            print(traceback.format_exc())
            return
