from pprint import pprint as pp
from datetime import date

from infrastructure.event_sourced_projections.board_lead_time_projection import LeadTimeProjection
from infrastructure.event_sourced_repos.board_repository import BoardRepository
from infrastructure.event_sourced_repos.work_item_repository import WorkItemRepository
from infrastructure.event_store import EventStore
from infrastructure.domain_event_subscriber import PersistenceSubscriber

from kanban.domain.model.board import start_project
from kanban.domain.model.workitem import register_new_work_item
from kanban.domain.services.overdue import locate_overdue_work_items


def main():

    es = EventStore("store.events")
    ps = PersistenceSubscriber(es)

    board = start_project("Test", "A test project")
    board_id = board.id

    board.name = "Another name"
    board.description = "A different description"

    todo_column = board.add_new_column("To do", 20)
    doing_column = board.add_new_column("Doing", 3)
    done_column = board.add_new_column("Done", None)

    #todo_column = board.column_with_name("To do")
    #impeded_column = board.insert_new_column_before(todo_column, "Impeded", 7)

    #board.remove_column(doing_column)

    print(repr(doing_column))

    work_item_1 = register_new_work_item(name="Feature 1",
                                         due_date=date(2013, 1, 12),
                                         content="Here's some info about how to make feature 1")

    work_item_2 = register_new_work_item(name="Feature 2",
                                         due_date=date(2014, 8, 13),
                                         content="Here's some info about how to make feature 2")

    work_item_3 = register_new_work_item(name="Feature 3",
                                         due_date=None,
                                         content="Here's some info about how to make feature 3")

    work_item_4 = register_new_work_item(name="Feature 4",
                                         due_date=date(2015, 3, 2),
                                         content="Here's some info about how to make feature 3")

    work_item_4.due_date = None

    work_item_repo = WorkItemRepository(es)

    board.schedule_work_item(work_item_3)
    board.schedule_work_item(work_item_1)
    board.schedule_work_item(work_item_2)
    board.schedule_work_item(work_item_4)

    board.abandon_work_item(work_item_2)

    board.advance_work_item(work_item_3)
    board.advance_work_item(work_item_3)
    board.advance_work_item(work_item_4)

    board.retire_work_item(work_item_3)

    overdue_work_items = locate_overdue_work_items(board, work_item_repo)

    pp(list(overdue_work_items))

    board_repo = BoardRepository(es)
    board_2 = board_repo.board_with_id(board_id)

    lead_time_projection = LeadTimeProjection(board_id, es)
    print(lead_time_projection.average_lead_time)

    board.advance_work_item(work_item_1)
    board.advance_work_item(work_item_1)
    board.retire_work_item(work_item_1)
    print(lead_time_projection.average_lead_time)

    lead_time_projection.close()

    pass

if __name__ == '__main__':
    main()
