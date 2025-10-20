import sqlite3
import os

DB_NAME = "forms.db"

# ====================================================================
# ШАГ 1: СХЕМА, АДАПТИРОВАННАЯ ДЛЯ SQLITE
# ====================================================================
SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

-- ================================
-- D1. DICTS (справочники, без ENUM)
-- ================================

CREATE TABLE IF NOT EXISTS step_types (
  id            INTEGER PRIMARY KEY,
  code          VARCHAR(64) UNIQUE NOT NULL,
  title         VARCHAR(128) NOT NULL
);

CREATE TABLE IF NOT EXISTS field_data_types (
  id            INTEGER PRIMARY KEY,
  code          VARCHAR(64) UNIQUE NOT NULL,
  title         VARCHAR(128) NOT NULL
);

CREATE TABLE IF NOT EXISTS field_input_types (
  id            INTEGER PRIMARY KEY,
  code          VARCHAR(64) UNIQUE NOT NULL,
  title         VARCHAR(128) NOT NULL
);

CREATE TABLE IF NOT EXISTS compare_ops (
  id            INTEGER PRIMARY KEY,
  code          VARCHAR(64) UNIQUE NOT NULL,
  title         VARCHAR(128) NOT NULL
);

CREATE TABLE IF NOT EXISTS visibility_actions (
  id            INTEGER PRIMARY KEY,
  code          VARCHAR(32) UNIQUE NOT NULL,
  title         VARCHAR(128) NOT NULL
);

CREATE TABLE IF NOT EXISTS instance_statuses (
  id            INTEGER PRIMARY KEY,
  code          VARCHAR(64) UNIQUE NOT NULL,
  title         VARCHAR(128) NOT NULL
);

CREATE TABLE IF NOT EXISTS dictionaries (
  id            INTEGER PRIMARY KEY,
  code          VARCHAR(128) UNIQUE NOT NULL,
  title         VARCHAR(256) NOT NULL
);

CREATE TABLE IF NOT EXISTS dictionary_values (
  id            INTEGER PRIMARY KEY,
  dictionary_id BIGINT NOT NULL REFERENCES dictionaries(id) ON DELETE CASCADE,
  value_code    VARCHAR(256) NOT NULL,
  value_label   VARCHAR(256) NOT NULL,
  sort_order    INT NOT NULL DEFAULT 100,
  UNIQUE (dictionary_id, value_code)
);

-- ================================
-- F. ФОРМЫ → ШАГИ → ПОЛЯ
-- ================================

-- Создаем 'forms' СНАЧАЛА без start_step_id
CREATE TABLE IF NOT EXISTS forms (
  id            INTEGER PRIMARY KEY,
  code          VARCHAR(128) UNIQUE NOT NULL,
  title         VARCHAR(256) NOT NULL,
  description   TEXT,
  is_active     BOOLEAN NOT NULL DEFAULT TRUE,
  -- start_step_id будет добавлен через ALTER TABLE
  created_by    BIGINT,
  created_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS form_steps (
  id            INTEGER PRIMARY KEY,
  form_id       BIGINT NOT NULL REFERENCES forms(id) ON DELETE CASCADE,
  step_type_id  SMALLINT NOT NULL REFERENCES step_types(id),
  code          VARCHAR(128) NOT NULL,
  title         VARCHAR(256) NOT NULL,
  sort_order    INT NOT NULL DEFAULT 100,
  is_terminal   BOOLEAN NOT NULL DEFAULT FALSE,
  UNIQUE (form_id, code)
);

-- Теперь добавляем start_step_id в 'forms', т.к. 'form_steps' уже существует
ALTER TABLE forms
  ADD COLUMN start_step_id BIGINT REFERENCES form_steps(id);

CREATE TABLE IF NOT EXISTS step_fields (
  id              INTEGER PRIMARY KEY,
  step_id         BIGINT NOT NULL REFERENCES form_steps(id) ON DELETE CASCADE,
  code            VARCHAR(128) NOT NULL,
  title           VARCHAR(256) NOT NULL,
  data_type_id    SMALLINT NOT NULL REFERENCES field_data_types(id),
  input_type_id   SMALLINT NOT NULL REFERENCES field_input_types(id),
  dictionary_id   BIGINT REFERENCES dictionaries(id),
  is_required     BOOLEAN NOT NULL DEFAULT FALSE,
  default_hidden  BOOLEAN NOT NULL DEFAULT FALSE,
  sort_order      INT NOT NULL DEFAULT 100,
  UNIQUE (step_id, code)
);

CREATE TABLE IF NOT EXISTS field_options (
  id            INTEGER PRIMARY KEY,
  field_id      BIGINT NOT NULL REFERENCES step_fields(id) ON DELETE CASCADE,
  value_code    VARCHAR(256) NOT NULL,
  value_label   VARCHAR(256) NOT NULL,
  sort_order    INT NOT NULL DEFAULT 100,
  UNIQUE (field_id, value_code)
);

-- ================================
-- C. УСЛОВИЯ (БЕЗ JSON)
-- ================================

CREATE TABLE IF NOT EXISTS condition_groups (
  id            INTEGER PRIMARY KEY,
  form_id       BIGINT NOT NULL REFERENCES forms(id) ON DELETE CASCADE,
  logic_op      VARCHAR(8) NOT NULL CHECK (logic_op IN ('AND','OR')),
  description   VARCHAR(512)
);

CREATE TABLE IF NOT EXISTS conditions (
  id            INTEGER PRIMARY KEY,
  group_id      BIGINT NOT NULL REFERENCES condition_groups(id) ON DELETE CASCADE,
  field_id      BIGINT NOT NULL REFERENCES step_fields(id) ON DELETE RESTRICT,
  op_id         SMALLINT NOT NULL REFERENCES compare_ops(id),
  value_text    VARCHAR(1024),
  value_num     REAL,
  value_bool    BOOLEAN,
  value_date    DATE,
  option_code   VARCHAR(256),
  rhs_field_id  BIGINT REFERENCES step_fields(id) ON DELETE RESTRICT,
  position      INT NOT NULL DEFAULT 100
);

-- ================================
-- V. ВИДИМОСТЬ ПОЛЕЙ
-- ================================

CREATE TABLE IF NOT EXISTS field_visibility_rules (
  id                  INTEGER PRIMARY KEY,
  step_id             BIGINT NOT NULL REFERENCES form_steps(id) ON DELETE CASCADE,
  condition_group_id  BIGINT NOT NULL REFERENCES condition_groups(id) ON DELETE CASCADE,
  action_id           SMALLINT NOT NULL REFERENCES visibility_actions(id),
  priority            INT NOT NULL DEFAULT 100
);

CREATE TABLE IF NOT EXISTS visibility_targets (
  id                  INTEGER PRIMARY KEY,
  visibility_rule_id  BIGINT NOT NULL REFERENCES field_visibility_rules(id) ON DELETE CASCADE,
  field_id            BIGINT NOT NULL REFERENCES step_fields(id) ON DELETE CASCADE,
  sort_order          INT NOT NULL DEFAULT 100,
  UNIQUE (visibility_rule_id, field_id)
);

-- ================================
-- T. ПЕРЕХОДЫ МЕЖДУ ШАГАМИ
-- ================================

CREATE TABLE IF NOT EXISTS step_transitions (
  id                  INTEGER PRIMARY KEY,
  form_id             BIGINT NOT NULL REFERENCES forms(id) ON DELETE CASCADE,
  source_step_id      BIGINT NOT NULL REFERENCES form_steps(id) ON DELETE CASCADE,
  target_step_id      BIGINT NOT NULL REFERENCES form_steps(id) ON DELETE RESTRICT,
  condition_group_id  BIGINT NOT NULL REFERENCES condition_groups(id) ON DELETE CASCADE,
  priority            INT NOT NULL DEFAULT 100,
  description         VARCHAR(512)
);

-- ================================
-- R. РАНТАЙМ (ИНСТАНС ФОРМЫ, ОТВЕТЫ, ИСТОРИЯ, СООБЩЕНИЯ)
-- ================================

CREATE TABLE IF NOT EXISTS form_instances (
  id              INTEGER PRIMARY KEY,
  form_id         BIGINT NOT NULL REFERENCES forms(id) ON DELETE RESTRICT,
  user_id         BIGINT NOT NULL,
  status_id       SMALLINT NOT NULL REFERENCES instance_statuses(id),
  current_step_id BIGINT REFERENCES form_steps(id),
  started_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS instance_steps (
  id            INTEGER PRIMARY KEY,
  instance_id   BIGINT NOT NULL REFERENCES form_instances(id) ON DELETE CASCADE,
  step_id       BIGINT NOT NULL REFERENCES form_steps(id) ON DELETE RESTRICT,
  status_code   VARCHAR(32) NOT NULL,
  entered_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  left_at       DATETIME
);

CREATE TABLE IF NOT EXISTS instance_answers (
  id            INTEGER PRIMARY KEY,
  instance_id   BIGINT NOT NULL REFERENCES form_instances(id) ON DELETE CASCADE,
  field_id      BIGINT NOT NULL REFERENCES step_fields(id) ON DELETE RESTRICT,
  value_text    TEXT,
  value_num     REAL,
  value_bool    BOOLEAN,
  value_date    DATE,
  updated_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE (instance_id, field_id)
);

CREATE TABLE IF NOT EXISTS instance_answers_multi (
  id            INTEGER PRIMARY KEY,
  instance_id   BIGINT NOT NULL REFERENCES form_instances(id) ON DELETE CASCADE,
  field_id      BIGINT NOT NULL REFERENCES step_fields(id) ON DELETE RESTRICT,
  option_code   VARCHAR(256) NOT NULL,
  updated_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS instance_messages (
  id            INTEGER PRIMARY KEY,
  instance_id   BIGINT NOT NULL REFERENCES form_instances(id) ON DELETE CASCADE,
  user_id       BIGINT,
  author_type   VARCHAR(32) NOT NULL DEFAULT 'system',
  message       TEXT NOT NULL,
  created_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ================================
-- I. ИНДЕКСЫ (практичные)
-- ================================

CREATE INDEX IF NOT EXISTS idx_forms_active        ON forms(is_active);
CREATE INDEX IF NOT EXISTS idx_steps_form          ON form_steps(form_id, sort_order);
CREATE INDEX IF NOT EXISTS idx_fields_step         ON step_fields(step_id, sort_order);
CREATE INDEX IF NOT EXISTS idx_cond_groups_form    ON condition_groups(form_id);
CREATE INDEX IF NOT EXISTS idx_conditions_group    ON conditions(group_id, position);
CREATE INDEX IF NOT EXISTS idx_vis_rules_step      ON field_visibility_rules(step_id, priority);
CREATE INDEX IF NOT EXISTS idx_transitions_source  ON step_transitions(source_step_id, priority);
CREATE INDEX IF NOT EXISTS idx_inst_user_status    ON form_instances(user_id, status_id);
CREATE INDEX IF NOT EXISTS idx_answers_instance    ON instance_answers(instance_id);
CREATE INDEX IF NOT EXISTS idx_answers_multi_inst  ON instance_answers_multi(instance_id, field_id);
CREATE INDEX IF NOT EXISTS instance_steps_idx ON instance_steps (instance_id, step_id);


-- ================================
-- S. SEED (минимальный набор словарей)
-- ================================

INSERT INTO step_types (code, title) VALUES
  ('questionnaire','Questionnaire'),
  ('upload','File upload'),
  ('review','Review')
ON CONFLICT (code) DO UPDATE SET title = excluded.title;

INSERT INTO field_data_types (code, title) VALUES
  ('string','String'), ('text','Text'), ('integer','Integer'), ('decimal','Decimal'),
  ('boolean','Boolean'), ('date','Date'), ('datetime','Datetime')
ON CONFLICT (code) DO UPDATE SET title = excluded.title;

INSERT INTO field_input_types (code, title) VALUES
  ('input','Input'), ('textarea','Textarea'), ('select','Select'),
  ('multiselect','Multiselect'), ('checkbox','Checkbox'), ('datepicker','Date picker')
ON CONFLICT (code) DO UPDATE SET title = excluded.title;

INSERT INTO compare_ops (code, title) VALUES
  ('eq','Equals'), ('ne','Not equals'), ('gt','Greater than'), ('gte','Greater or equal'),
  ('lt','Less than'), ('lte','Less or equal'), ('in','In list'), ('not_in','Not in list'),
  ('like','Like'), ('ilike','Case-insensitive like'),
  ('is_true','Is true'), ('is_false','Is false'), ('is_empty','Is empty'), ('not_empty','Not empty')
ON CONFLICT (code) DO UPDATE SET title = excluded.title;

INSERT INTO visibility_actions (code, title) VALUES
  ('show','Show'), ('hide','Hide')
ON CONFLICT (code) DO UPDATE SET title = excluded.title;

INSERT INTO instance_statuses (code, title) VALUES
  ('draft','Draft'), ('in_progress','In progress'), ('paused','Paused'),
  ('completed','Completed'), ('cancelled','Cancelled')
ON CONFLICT (code) DO UPDATE SET title = excluded.title;
"""

# ====================================================================
# ШАГ 2: ДЕМО-ДАННЫЕ
# (Они были совместимы с SQLite, но теперь они зависят
#  от ID, которые создаются в п.1. Этот скрипт все еще
#  предполагает запуск на ПУСТОЙ базе)
# ====================================================================
DEMO_DATA_SQL = """
-- 1. Создаем Форму (id=1)
INSERT INTO forms (id, code, title, description, is_active)
VALUES (1, 'dev_survey', 'Анкета Разработчика', 'Простая анкета с ветвлением', TRUE)
ON CONFLICT(id) DO NOTHING;

-- 2. Создаем Шаги (id=10, 20, 30)
INSERT INTO form_steps (id, form_id, step_type_id, code, title, is_terminal)
VALUES
  (10, 1, (SELECT id FROM step_types WHERE code = 'questionnaire'), 'step_1_intro', 'Шаг 1: Интро', FALSE),
  (20, 1, (SELECT id FROM step_types WHERE code = 'questionnaire'), 'step_2_dev', 'Шаг 2: Детали', FALSE),
  (30, 1, (SELECT id FROM step_types WHERE code = 'review'), 'step_3_review', 'Шаг 3: Обзор', TRUE)
ON CONFLICT(id) DO NOTHING;

-- 3. Назначаем стартовый шаг для Формы 1
-- (Это обновление теперь сработает, т.к. колонка была добавлена)
UPDATE forms SET start_step_id = 10 WHERE id = 1;

-- 4. Создаем Поля (id=101, 201)
INSERT INTO step_fields (id, step_id, code, title, data_type_id, input_type_id, is_required, default_hidden)
VALUES
  (101, 10, 'is_dev', 'Вы разработчик?', (SELECT id FROM field_data_types WHERE code = 'boolean'), (SELECT id FROM field_input_types WHERE code = 'checkbox'), TRUE, FALSE),
  (201, 20, 'fav_lang', 'Любимый язык?', (SELECT id FROM field_data_types WHERE code = 'string'), (SELECT id FROM field_input_types WHERE code = 'select'), TRUE, FALSE),
  (202, 20, 'dev_years', 'Сколько лет вы работаете разработчиком?',
      (SELECT id FROM field_data_types WHERE code = 'integer'),
      (SELECT id FROM field_input_types WHERE code = 'input'),
      FALSE, TRUE)
ON CONFLICT(id) DO NOTHING;

-- 5. Добавляем опции для поля "fav_lang" (id=201)
INSERT INTO field_options (field_id, value_code, value_label)
VALUES
  (201, 'py', 'Python'),
  (201, 'go', 'Go'),
  (201, 'js', 'JavaScript')
ON CONFLICT(field_id, value_code) DO NOTHING;

-- 6. Создаем УСЛОВИЯ
-- Группа 1: "is_dev == true"
INSERT INTO condition_groups (id, form_id, logic_op, description)
VALUES (1, 1, 'AND', 'Разработчик = Да')
ON CONFLICT(id) DO NOTHING;

INSERT INTO conditions (group_id, field_id, op_id)
VALUES (1, 101, (SELECT id FROM compare_ops WHERE code = 'is_true'))
ON CONFLICT(id) DO NOTHING;

-- Группа 2: "is_dev == false"
INSERT INTO condition_groups (id, form_id, logic_op, description)
VALUES (2, 1, 'AND', 'Разработчик = Нет')
ON CONFLICT(id) DO NOTHING;

INSERT INTO conditions (group_id, field_id, op_id)
VALUES (2, 101, (SELECT id FROM compare_ops WHERE code = 'is_false'))
ON CONFLICT(id) DO NOTHING;

-- Правила видимости: показываем "dev_years", если пользователь разработчик
INSERT INTO field_visibility_rules (id, step_id, condition_group_id, action_id, priority)
VALUES (1, 20, 1, (SELECT id FROM visibility_actions WHERE code = 'show'), 10)
ON CONFLICT(id) DO NOTHING;

INSERT INTO visibility_targets (visibility_rule_id, field_id)
VALUES (1, 202)
ON CONFLICT(visibility_rule_id, field_id) DO NOTHING;

-- Группа 99: "Default"
INSERT INTO condition_groups (id, form_id, logic_op, description)
VALUES (99, 1, 'AND', 'По умолчанию для шага 2')
ON CONFLICT(id) DO NOTHING;

INSERT INTO conditions (group_id, field_id, op_id)
VALUES (99, 201, (SELECT id FROM compare_ops WHERE code = 'not_empty'))
ON CONFLICT(id) DO NOTHING;


-- 7. Создаем ПЕРЕХОДЫ
-- Переход 1: (Шаг 1 -> Шаг 2), если is_dev = true
INSERT INTO step_transitions (form_id, source_step_id, target_step_id, condition_group_id, priority)
VALUES (1, 10, 20, 1, 10)
ON CONFLICT(id) DO NOTHING;

-- Переход 2: (Шаг 1 -> Шаг 3), если is_dev = false
INSERT INTO step_transitions (form_id, source_step_id, target_step_id, condition_group_id, priority)
VALUES (1, 10, 30, 2, 20)
ON CONFLICT(id) DO NOTHING;

-- Переход 3: (Шаг 2 -> Шаг 3)
INSERT INTO step_transitions (form_id, source_step_id, target_step_id, condition_group_id, priority)
VALUES (1, 20, 30, 99, 100)
ON CONFLICT(id) DO NOTHING;

CREATE TABLE IF NOT EXISTS field_visibility_targets (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  rule_id INTEGER NOT NULL REFERENCES field_visibility_rules(id) ON DELETE CASCADE,
  field_id INTEGER NOT NULL REFERENCES step_fields(id) ON DELETE CASCADE
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_visibility_target ON field_visibility_targets(rule_id, field_id);
"""


def migrate():
    """Создает БД и таблицы, заполняет демо-данными."""
    if os.path.exists(DB_NAME):
        print(f"Удаление старой БД {DB_NAME}...")
        os.remove(DB_NAME)

    print(f"Создание новой БД {DB_NAME}...")
    # 'isolation_level=None' для авто-коммита DDL
    with sqlite3.connect(DB_NAME, isolation_level=None) as con:
        cur = con.cursor()

        # Включаем поддержку FOREIGN KEY для SQLite
        cur.execute("PRAGMA foreign_keys = ON;")

        print("1. Применение схемы (SQLite-совместимая)...")
        # executescript выполняет сразу весь SQL-блок
        cur.executescript(SCHEMA_SQL)
        print("Схема успешно применена.")

        print("2. Заполнение демо-данными...")
        cur.executescript(DEMO_DATA_SQL)
        print("Демо-данные успешно загружены.")

    print("\nМиграция завершена. База данных готова.")
    print("Создана анкета 'dev_survey' (id=1) с условным переходом.")


if __name__ == "__main__":
    migrate()