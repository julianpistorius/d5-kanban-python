from infrastructure.event_sourced_repos.board_repository import BoardRepository
from infrastructure.event_store import EventStore
from infrastructure.message_hub import MessageHub
from infrastructure.domain_event_subscriber import PersistenceSubscriber

from kanban.domain.model.board import start_project


def main():

    hub = MessageHub()
    es = EventStore("store.events")
    ps = PersistenceSubscriber(hub, es)

    board = start_project("Test", "A test project", hub)
    board_id = board.id

    board.name = "Another name"
    board.description = "A different description"

    board.add_new_column("To do", 20)
    doing_column = board.add_new_column("Doing", 3)
    done_column = board.add_new_column("Done", None)

    todo_column = board.column_with_name("To do")
    impeded_column = board.insert_new_column_before(todo_column, "Impeded", 7)


    board.remove_column(doing_column)

    print(repr(doing_column))

    board_repo = BoardRepository(es, hub)
    board_2 = board_repo.board_with_id(board_id)



    pass

if __name__ == '__main__':
    main()
