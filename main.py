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
    board.add_new_column("Done", 30)

    todo_column = board.column_with_name("To do")

    print(repr(doing_column))

    board.remove_column(doing_column)

    print(repr(doing_column))
    #print(doing_column.id)

    #board.discard()

    #print(todo_column.name)

    #print(board.id)

    board_repo = BoardRepository(es, hub)
    board_2 = board_repo.board_with_id(board_id)

    board_3 = board_repo.board_with_id(board_id)

    assert board_2 is board_3

    pass

if __name__ == '__main__':
    main()
