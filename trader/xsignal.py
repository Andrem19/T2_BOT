from database.signaler import Signaler


def get_signal(signals: Signaler):
    if signals.score > 0.30 and signals.rules_count > 3:
        return 1
    elif signals.score < -0.30 and signals.rules_count > 3:
        return 2
    else: 
        return 0
        