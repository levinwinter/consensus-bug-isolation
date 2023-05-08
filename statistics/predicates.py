def pred_branch(predicate, cond, ln):
    try:
        result = bool(predicate)
        print(result, f'{cond} == True (ln. {ln})')
        print(not result, f'{cond} == False (ln. {ln})')
    except:
        pass
    return predicate


def pred_returns(value, call, ln):
    try:
        print(value == 0, f'{call} == 0 (ln. {ln})')
    except:
        pass
    try:
        print(value != 0, f'{call} != 0 (ln. {ln})')
    except:
        pass
    try:
        print(value < 0, f'{call} < 0 (ln. {ln})')
    except:
        pass
    try:
        print(value <= 0, f'{call} <= 0 (ln. {ln})')
    except:
        pass
    try:
        print(value > 0, f'{call} > 0 (ln. {ln})')
    except:
        pass
    try:
        print(value >= 0, f'{call} >= 0 (ln. {ln})')
    except:
        pass
    return value
