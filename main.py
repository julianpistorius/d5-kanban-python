from datetime import date
from infrastructure.event_sourced_repos.board_repository import BoardRepository
from infrastructure.event_sourced_repos.work_item_repository import WorkItemRepository
from infrastructure.event_store import EventStore
from infrastructure.message_hub import MessageHub
from infrastructure.domain_event_subscriber import PersistenceSubscriber

from kanban.domain.model.board import start_project
from kanban.domain.model.workitem import register_new_work_item


def main():

    hub = MessageHub()
    es = EventStore("store.events")
    ps = PersistenceSubscriber(hub, es)

    board = start_project("Test", "A test project", hub)
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
                                         due_date=date(2014, 8, 13),
                                         content="Here's some info about how to make feature 1",
                                         hub=hub)

    work_item_2 = register_new_work_item(name="Feature 2",
                                         due_date=date(2014, 8, 13),
                                         content="Here's some info about how to make feature 2",
                                         hub=hub)

    work_item_3 = register_new_work_item(name="Feature 3",
                                         due_date=date(2014, 8, 13),
                                         content="Here's some info about how to make feature 3",
                                         hub=hub)

    work_item_repo = WorkItemRepository(es, hub)

    board.schedule_work_item(work_item_3)
    board.schedule_work_item(work_item_1)
    board.schedule_work_item(work_item_2)

    board.abandon_work_item(work_item_2)

    board_repo = BoardRepository(es, hub)
    board_2 = board_repo.board_with_id(board_id)

    pass

if __name__ == '__main__':
    main()
