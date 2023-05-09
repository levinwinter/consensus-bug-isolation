import math


class Aggregation:

    def __init__(self):
        self.failure_true = 0
        self.failure_false = 0
        self.successful_true = 0
        self.successful_false = 0

    def failure(self):
        observed_true = self.failure_true + self.successful_true
        if observed_true == 0:
            return 0
        return self.failure_true / observed_true

    def context(self):
        observed = self.failure_true + self.failure_false + \
            self.successful_true + self.successful_false
        if observed == 0:
            return 0
        return (self.failure_true + self.failure_false) / observed

    def increase(self):
        return self.failure() - self.context()

    def importance(self, total_failures):
        increase = self.increase()
        if increase == 0 or self.failure_true == 1 or total_failures == 1:
            return 0
        return 2 / ((1 / increase) + (1 / (math.log(self.failure_true) / math.log(total_failures))))

    def __str__(self) -> str:
        return f"(f_true: {self.failure_true}, f_false: {self.failure_false}, s_true: {self.successful_true}, s_false: {self.successful_false})"


class Report:

    def __init__(self, successful, observations: dict[str, bool]):
        self.successful = successful
        self.observations = observations


def aggregate(reports: list[Report]):
    aggregation: dict[str, Aggregation] = {}

    for report in reports:
        for (id, observed_true) in report.observations.items():
            if id not in aggregation:
                aggregation[id] = Aggregation()
            if report.successful:
                aggregation[id].successful_true += 1 if observed_true else 0
                aggregation[id].successful_false += 0 if observed_true else 1
            else:
                aggregation[id].failure_true += 1 if observed_true else 0
                aggregation[id].failure_false += 0 if observed_true else 1

    return aggregation


def filter_increase(aggregations: dict[str, Aggregation]):
    filtered = {}
    for (id, agg) in aggregations.items():
        if agg.increase() > 0:  # TODO use confidence interval
            filtered[id] = agg
    return filtered


def most_important(aggregations, total_failures):
    def importance(item): return item[1].importance(total_failures)
    id, _ = max(aggregations.items(), key=importance)
    return id


def isolate(reports: list[Report]):
    aggregations = aggregate(reports)
    filtered = filter_increase(aggregations)

    if len(filtered) == 0:
        return

    failed_reports = list(
        filter(lambda report: not report.successful, reports))
    total_failures = len(failed_reports)

    predicate = most_important(filtered, total_failures)
    print(predicate)

    filtered_reports = []
    for report in reports:
        if predicate not in report.observations or not report.observations[predicate]:
            filtered_reports.append(report)

    isolate(filtered_reports)
