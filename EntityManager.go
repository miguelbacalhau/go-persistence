// Package persistance provide TODO
package persistence

type DriverInterface interface {
	// TODO is this needed???
}
type EntityManagerInterface interface {
	GetRepository(name string) RepositoryInterface
}

type RepositoryInterface interface {
	Find(id int) (EntityInterface, error)
	FindAll() []EntityInterface
	Add(entity EntityInterface)
}

type Repository struct {
	Entities []EntityInterface
}

func (repository *Repository) Find(id int) (EntityInterface, error) {
	var err error
	var entity EntityInterface

	for index, value := range repository.Entities {
		if id == index {
			entity = value
			break
		}
	}

	return entity, err
}

func (repository *Repository) FindAll() []EntityInterface {
	return repository.Entities
}

func (repository *Repository) Add(entity EntityInterface) {
	entities := repository.Entities
	repository.Entities = append(entities, entity)
}

type EntityInterface interface {
	GetId() int
}
