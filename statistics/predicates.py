def pred_branch(value, id, ln):
    try:
        result = bool(value)
        print(result, f'{id} == True (ln. {ln})')
        print(not result, f'{id} == False (ln. {ln})')
    except:
        pass
    return value


def pred_returns(value, id, ln):
    try:
        print(value == 0, f'{id} == 0 (ln. {ln})')
    except:
        pass
    try:
        print(value != 0, f'{id} != 0 (ln. {ln})')
    except:
        pass
    try:
        print(value < 0, f'{id} < 0 (ln. {ln})')
    except:
        pass
    try:
        print(value <= 0, f'{id} <= 0 (ln. {ln})')
    except:
        pass
    try:
        print(value > 0, f'{id} > 0 (ln. {ln})')
    except:
        pass
    try:
        print(value >= 0, f'{id} >= 0 (ln. {ln})')
    except:
        pass
    return value
