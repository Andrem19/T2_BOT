import shared_vars as sv


def define_sim_pos(new_simulation: dict):
    pos_1 = sv.stages['simulation']['position_1']
    pos_2 = sv.stages['simulation']['position_2']
    
    sv.stages['simulation']['position_1'] = new_simulation
    sv.stages['simulation']['position_2'] = pos_1
    sv.stages['simulation']['position_3'] = pos_2