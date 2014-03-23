from infrastructure.event_sourced_repos.board_repository import BoardRepository
from infrastructure.event_store import EventStore
from kanban.domain.model.board import start_project
from infrastructure.domain_event_subscriber import PersistenceSubscriber
from kanban.events import hub

def main():

    es = EventStore("store.events")
    ps = PersistenceSubscriber(hub(), es)

    board = start_project("Test", "A test project")
    board_id = board.id

    board.name = "Another name"
    board.description = "A different description"

    board.add_new_column("To do", 20)
    board.add_new_column("Doing", 3)
    board.add_new_column("Done", 30)

    #board.discard()

    #code.interact(local=locals())

    board_repo = BoardRepository(es)
    board_2 = board_repo.board_with_id(board_id)

    pass

if __name__ == '__main__':
    main()
