package storage

import (
	"context"

	"github.com/jackc/pgx/v4/pgxpool"
)

/*
API пакета storage должен позволять:
DONE Создавать новые задачи, - func NewTask
DONE Получать список всех задач, - func Tasks (если передать taskID=0 и authorID=0 то будут выведены все задачи)
DONE Получать список задач по автору, - func TaskByAuthor
DONE Получать список задач по метке, - func TaskByLabel
DONE Обновлять задачу по id, - func UpdateTask
DONE Удалять задачу по id. - func DeleteTask
*/

// Хранилище данных.
type Storage struct {
	db *pgxpool.Pool
}

// Конструктор, принимает строку подключения к БД.
func New(constr string) (*Storage, error) {
	db, err := pgxpool.Connect(context.Background(), constr)
	if err != nil {
		return nil, err
	}
	s := Storage{
		db: db,
	}
	return &s, nil
}

// Задача.
type Task struct {
	ID         int
	Opened     int64
	Closed     int64
	AuthorID   int
	AssignedID int
	Title      string
	Content    string
}

// Tasks возвращает список задач из БД.
func (s *Storage) Tasks(taskID, authorID int) ([]Task, error) {
	rows, err := s.db.Query(context.Background(), `
		SELECT 
			id,
			opened,
			closed,
			author_id,
			assigned_id,
			title,
			content
		FROM tasks
		WHERE
			($1 = 0 OR id = $1) AND
			($2 = 0 OR author_id = $2)
		ORDER BY id;
	`,
		taskID,
		authorID,
	)
	if err != nil {
		return nil, err
	}
	var tasks []Task
	// итерирование по результату выполнения запроса
	// и сканирование каждой строки в переменную
	for rows.Next() {
		var t Task
		err = rows.Scan(
			&t.ID,
			&t.Opened,
			&t.Closed,
			&t.AuthorID,
			&t.AssignedID,
			&t.Title,
			&t.Content,
		)
		if err != nil {
			return nil, err
		}
		// добавление переменной в массив результатов
		tasks = append(tasks, t)

	}
	// ВАЖНО не забыть проверить rows.Err()
	return tasks, rows.Err()
}

// NewTask создаёт новую задачу и возвращает её id.
func (s *Storage) NewTask(t Task) (int, error) {
	var id int
	err := s.db.QueryRow(context.Background(), `
		INSERT INTO tasks (title, content)
		VALUES ($1, $2) RETURNING id;
		`,
		t.Title,
		t.Content,
	).Scan(&id)
	return id, err
}

// TaskByAuthor возвращает список задач определенного автора.
func (s *Storage) TaskByAuthor(authorID int) ([]Task, error) {
	rows, err := s.db.Query(context.Background(), `
		SELECT 
			id,
			opened,
			closed,
			author_id,
			assigned_id,
			title,
			content
		FROM tasks
		WHERE
			(author_id = $1)
		ORDER BY id;
	`,
		authorID,
	)
	if err != nil {
		return nil, err
	}
	var tasks []Task
	// итерирование по результату выполнения запроса
	// и сканирование каждой строки в переменную
	for rows.Next() {
		var t Task
		err = rows.Scan(
			&t.ID,
			&t.Opened,
			&t.Closed,
			&t.AuthorID,
			&t.AssignedID,
			&t.Title,
			&t.Content,
		)
		if err != nil {
			return nil, err
		}
		// добавление переменной в массив результатов
		tasks = append(tasks, t)

	}
	// ВАЖНО не забыть проверить rows.Err()
	return tasks, rows.Err()
}

// TaskByLabel возвращает список задач с соответствующей меткой.
func (s *Storage) TaskByLabel(labelName string) ([]Task, error) {
	rows, err := s.db.Query(context.Background(), `
		SELECT 
			id,
			opened,
			closed,
			author_id,
			assigned_id,
			title,
			content
		FROM tasks
		WHERE id IN (select task_id from tasks_labels where label_id in 
			(select id from labels where name = $1)
		ORDER BY id;
	`,
		labelName,
	)
	if err != nil {
		return nil, err
	}
	var tasks []Task
	// итерирование по результату выполнения запроса
	// и сканирование каждой строки в переменную
	for rows.Next() {
		var t Task
		err = rows.Scan(
			&t.ID,
			&t.Opened,
			&t.Closed,
			&t.AuthorID,
			&t.AssignedID,
			&t.Title,
			&t.Content,
		)
		if err != nil {
			return nil, err
		}
		// добавление переменной в массив результатов
		tasks = append(tasks, t)

	}
	// ВАЖНО не забыть проверить rows.Err()
	return tasks, rows.Err()
}

// UpdateTask обновляет поля задачи и возвращает задачу.
func (s *Storage) UpdateTask(taskData Task) (Task, error) {
	var updatedTask Task
	err := s.db.QueryRow(context.Background(), `
			UPDATE tasks
			SET assigned_id = $1,
				closed = $2,
				content = $3,
				title = $4
			WHERE id = $5
			RETURNING id, opened, closed, author_id, assigned_id, title, content;
			`,
		taskData.AssignedID,
		taskData.Closed,
		taskData.Content,
		taskData.Title,
		taskData.ID,
	).Scan(&updatedTask.ID, &updatedTask.Opened, &updatedTask.Closed, &updatedTask.AuthorID, &updatedTask.AssignedID, &updatedTask.Title, &updatedTask.Content)

	if err != nil {
		return Task{}, err
	}

	return updatedTask, nil
}

// DeleteTask удаляет задачу по id.
func (s *Storage) DeleteTask(id int) error {

	_, err := s.db.Query(context.Background(), `
			DELETE FROM tasks
			WHERE id = $1;
			`,
		id,
	)

	if err != nil {
		return err
	}

	return nil
}
