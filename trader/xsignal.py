from database.signaler import Signaler


def get_signal(signals: Signaler):
    if signals.score > 0.80:
        return 1
    elif signals.score < 0.20:
        return 2
    else: 
        return 0
        