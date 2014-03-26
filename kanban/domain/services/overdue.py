"""Domain services for finding overdue work items.
"""

import datetime
from itertools import chain

from utility.utilities import utc_now


def locate_overdue_work_items(board, work_item_repo):
    """Obtain work items which have been scheduled on a Board, but which are overdue.

    Overdue is computed with respect to the current date ("today") and the due date associated with the WorkItem.

    Args:
        board: The board for which overdue work items are to be located.
        work_item_repo: A repository from which work items can be retrieved.

    Returns:
        An iterable series of WorkItems.
    """
    today = datetime.datetime.fromtimestamp(utc_now()).date()

    def _overdue(work_item):
        return work_item.due_date < today

    work_item_ids = chain.from_iterable(column.work_item_ids() for column in board.columns())
    overdue_work_items = work_item_repo.work_items_where(_overdue, work_item_ids)
    return overdue_work_items


def any_overdue_work_items(board, work_item_repo):
    """Determine whether there are any overdue work items scheduled on a board.

    Overdue is computed with respect to the current date ("today") and the due date associated with the WorkItem.

    Args:
        board: The board for which overdue work items are to be located.
        work_item_repo: A repository from which work items can be retrieved.

    Returns:
        True if there are overdue work items, otherwise False.
    """
    overdue_work_items = locate_overdue_work_items(board, work_item_repo)
    return any(True for _ in overdue_work_items)
