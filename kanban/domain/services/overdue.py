import datetime
from itertools import chain

from utility.utilities import utc_now


def locate_overdue_work_items(board, work_item_repo):
    today = datetime.datetime.fromtimestamp(utc_now()).date()

    def _overdue(work_item):
        return work_item.due_date < today

    work_item_ids = chain.from_iterable(column.work_item_ids() for column in board.columns())
    overdue_work_items = work_item_repo.work_items_where(_overdue, work_item_ids)
    return overdue_work_items
