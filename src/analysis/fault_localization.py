import math


class Aggregation:

    def __init__(self):
        self.failure_true = set()
        self.failure_false = set()
        self.successful_true = set()
        self.successful_false = set()

    def failure(self):
        observed_true = len(self.failure_true) + len(self.successful_true)
        if observed_true == 0:
            return 0
        return len(self.failure_true) / observed_true

    def context(self):
        observed = len(self.failure_true) + len(self.failure_false) + \
            len(self.successful_true) + len(self.successful_false)
        if observed == 0:
            return 0
        return (len(self.failure_true) + len(self.failure_false)) / observed

    def increase(self):
        return self.failure() - self.context()

    def importance(self, total_failures):
        increase = self.increase()
        if increase == 0 or len(self.failure_true) == 1 or total_failures == 1:
            return 0
        return 2 / ((1 / increase) + (1 / (math.log(len(self.failure_true)) / math.log(total_failures))))

    def __str__(self) -> str:
        return f"(f_true: {len(self.failure_true)}, f_false: {len(self.failure_false)}, s_true: {len(self.successful_true)}, s_false: {len(self.successful_false)})"
    
    def __eq__(self, __value: object) -> bool:
        if not isinstance(__value, Aggregation):
            return False
        other: Aggregation = __value
        return self.successful_true == other.successful_true and self.successful_false == other.successful_false and self.failure_true == other.failure_true and self.failure_false == other.failure_false


class Report:

    def __init__(self, successful, observations: dict[str, bool], name):
        self.successful = successful
        self.observations = observations
        self.name = name


def aggregate(reports: list[Report]):
    aggregation: dict[str, Aggregation] = {}

    for report in reports:
        for (id, observed_true) in report.observations.items():
            if id not in aggregation:
                aggregation[id] = Aggregation()
            if report.successful and observed_true:
                aggregation[id].successful_true.add(report.name)
            elif report.successful and not observed_true:
                aggregation[id].successful_false.add(report.name)
            elif not report.successful and observed_true:
                aggregation[id].failure_true.add(report.name)
            elif not report.successful and not observed_true:
                aggregation[id].failure_false.add(report.name)

    return aggregation


def filter_increase(aggregations: dict[str, Aggregation]):
    filtered = {}
    for (id, agg) in aggregations.items():
        if agg.increase() > 0:  # TODO use confidence interval
            filtered[id] = agg
    return filtered


def most_important(aggregations, total_failures):
    def increase(item): return item[1].increase()
    def importance(item): return item[1].importance(total_failures)
    # for (x, y) in list(sorted(aggregations.items(), key=importance, reverse=True))[:25]:
    #     # if y.failure_false == 0 and y.successful_true == 0:
    #     print(y.importance(total_failures), x, y)
    # # sort = sorted(aggregations.items(), key=lambda x: x[1].failure_true, reverse=True)
    sort = aggregations.items()
    id, agg = max(sort, key=importance)
    # for (x, y) in list(sorted(aggregations.items(), key=importance, reverse=True))[:50]:
    #     if importance((x, y)) == importance((id, agg)):
    #         print('same importance', x, y)
    return id, agg


def isolate(reports: list[Report], aggregations:dict[str, Aggregation]={}, stats_fn=None):
    print(len(reports), 'reports')
    if isinstance(reports[0], Report):
        aggregations = aggregate(reports)
    print(len(aggregations), 'aggregations')
    # for id, aggregation in aggregations.items():
    #     if aggregation.successful_false > 0:
    #         print(id, str(aggregation))
    filtered = filter_increase(aggregations)
    print(len(filtered), 'filtered aggregations')

    if len(filtered) == 0:
        return

    failed_reports = list(
        filter(lambda report: not report.successful, reports))
    total_failures = len(failed_reports)
    print(total_failures, 'failed reports')

    if failed_reports == 0:
        return

    predicate, agg = most_important(filtered, total_failures)
    print(agg.importance(total_failures), predicate, str(agg))

    predicates = []
    predicates = [(predicate, agg)]

    # threshold = int(predicate.split(' ')[2])
    # tolerance = int(predicate.split(' > ')[1])

    # print(predicate)

    # for (x, y) in aggregations.items():
    #     if ' '.join(x.split(' ')[3:-1]) == ' '.join(predicate.split(' ')[3:-1]) and int(x.split(' ')[2]) >= threshold and int(x.split(' > ')[1]) >= tolerance:
    #         predicates.append((x, y))
    #         print(x)

    removed = set()
    for (x, y) in predicates:
        removed = removed.union(y.failure_true)
    filtered_reports = list(filter(lambda report: report.name not in agg.failure_true, reports))
    print('removed', len(removed), ", ".join(removed))

    if stats_fn:
        stats_fn(removed)

    for (predicate, y) in predicates:
        aggregations.pop(predicate)

    for aggregation in aggregations.values():
        aggregation.successful_true.difference_update(removed)
        aggregation.successful_false.difference_update(removed)
        aggregation.failure_true.difference_update(removed)
        aggregation.failure_false.difference_update(removed)

    isolate(filtered_reports, aggregations, stats_fn)
