import shared_vars as sv


def get_required_position():
    """
    Определяет, какая позиция требуется, на основании полей объекта sv.

    Args:
        sv: объект с атрибутами
            - close_1 (bool)
            - close_2 (bool)
            - stages (dict), где stages['first']['exist'] и stages['second']['exist'] — булевы флаги

    Returns:
        str: одна из {'nothing', 'first', 'second'}
    """
    # Значение по умолчанию
    which_pos_we_need = 'nothing'

    if (sv.close_1 and sv.close_2) or (
        sv.stages['second']['exist'] and not sv.stages['first']['exist']
    ):
        which_pos_we_need = 'nothing'
    elif (
        not sv.stages['first']['exist']
        and not sv.stages['second']['exist']
        and sv.close_1
        and not sv.close_2
    ):
        which_pos_we_need = 'second'
    elif (
        not sv.stages['first']['exist']
        and not sv.stages['second']['exist']
        and sv.close_2
        and not sv.close_1
    ):
        which_pos_we_need = 'first'
    elif (
        not sv.stages['first']['exist']
        and not sv.stages['second']['exist']
        and not sv.close_1
        and not sv.close_2
    ):
        which_pos_we_need = 'first'
    elif (
        not sv.stages['second']['exist']
        and sv.stages['first']['exist']
        and not sv.close_2
    ):
        which_pos_we_need = 'second'

    return which_pos_we_need
