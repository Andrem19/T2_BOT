from database.signaler import Signaler


def get_signal(signals: Signaler):
    if signals.score > 0.80 and signals.yscore > 0.25:
        return 1
    elif signals.score < 0.30 and signals.yscore < -0.30:
        return 2
    else: 
        return 0
        