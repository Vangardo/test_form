import uvicorn
from fastapi import FastAPI, HTTPException, Depends, Body, status
from pydantic import BaseModel, Field, model_validator
from typing import List, Any, Dict, Optional, Tuple
import databases
import sqlite3
from contextlib import asynccontextmanager
from pathlib import Path
from fastapi.responses import FileResponse

INDEX_HTML = Path(__file__).with_name("index.html")
# ====================================================================
# 1. НАСТРОЙКА БАЗЫ ДАННЫХ
# ====================================================================
DATABASE_URL = "sqlite:///./forms.db"
database = databases.Database(DATABASE_URL)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # При старте
    await database.connect()
    # Включаем поддержку FOREIGN KEY для SQLite на уровне подключения
    await database.execute("PRAGMA foreign_keys = ON;")
    yield
    # При выключении
    await database.disconnect()


app = FastAPI(
    title="Полный Form Engine API (Админка + Рантайм + Справочники)",
    lifespan=lifespan
)


# ====================================================================
# 2. HELPER-ФУНКЦИИ ДЛЯ АДМИНКИ (ПОИСК ID)
# ====================================================================

async def _get_id_by_code(table_name: str, code: str) -> int:
    """Универсальный помощник для получения ID из справочников."""
    query = f"SELECT id FROM {table_name} WHERE code = :code"
    row = await database.fetch_one(query, {"code": code})
    if not row:
        raise HTTPException(status_code=404, detail=f"Код '{code}' не найден в таблице '{table_name}'")
    return row.id


async def _get_field_id_by_code(form_id: int, field_code: str) -> int:
    """Находит Field ID по его коду в рамках всей формы."""
    query = """
        SELECT f.id
        FROM step_fields f
        JOIN form_steps s ON f.step_id = s.id
        WHERE s.form_id = :form_id AND f.code = :field_code
    """
    row = await database.fetch_one(query, {"form_id": form_id, "field_code": field_code})
    if not row:
        raise HTTPException(status_code=404, detail=f"Поле с кодом '{field_code}' не найдено в форме {form_id}")
    return row.id


async def _ensure_step_in_form(form_id: int, step_id: int) -> None:
    row = await database.fetch_one(
        "SELECT id FROM form_steps WHERE id = :step_id AND form_id = :form_id",
        {"step_id": step_id, "form_id": form_id}
    )
    if not row:
        raise HTTPException(status_code=404, detail=f"Шаг {step_id} не найден в форме {form_id}")


async def _fetch_step(step_id: int) -> StepRead:
    query = """
        SELECT s.*, st.code AS step_type_code
        FROM form_steps s
        JOIN step_types st ON s.step_type_id = st.id
        WHERE s.id = :step_id
    """
    row = await database.fetch_one(query, {"step_id": step_id})
    if not row:
        raise HTTPException(status_code=404, detail=f"Шаг {step_id} не найден")
    return StepRead(**row)


async def _fetch_route(route_id: int, form_id: int) -> StepRouteRead:
    query_route = """
        SELECT t.id, t.form_id, t.source_step_id, t.target_step_id, t.priority,
               t.description, g.id AS condition_group_id, g.logic_op,
               g.description AS scenario_description
        FROM step_transitions t
        JOIN condition_groups g ON t.condition_group_id = g.id
        WHERE t.id = :route_id AND t.form_id = :form_id
    """
    route_row = await database.fetch_one(query_route, {"route_id": route_id, "form_id": form_id})
    if not route_row:
        raise HTTPException(status_code=404, detail=f"Переход {route_id} не найден в форме {form_id}")

    query_conditions = """
        SELECT c.id, c.field_id, c.op_id, c.value_text, c.value_num, c.value_bool,
               c.value_date, c.option_code, c.position,
               sf.code AS field_code, sf.title AS field_title,
               rhs.code AS rhs_field_code,
               co.code AS op_code, co.title AS op_title
        FROM conditions c
        JOIN step_fields sf ON c.field_id = sf.id
        JOIN compare_ops co ON c.op_id = co.id
        LEFT JOIN step_fields rhs ON c.rhs_field_id = rhs.id
        WHERE c.group_id = :group_id
        ORDER BY c.position
    """
    condition_rows = await database.fetch_all(
        query_conditions, {"group_id": route_row.condition_group_id}
    )

    conditions = [
        ConditionRead(
            id=row.id,
            field_code=row.field_code,
            field_title=row.field_title,
            op_code=row.op_code,
            op_title=row.op_title,
            value_text=row.value_text,
            value_num=row.value_num,
            value_bool=row.value_bool,
            value_date=row.value_date,
            option_code=row.option_code,
            rhs_field_code=row.rhs_field_code,
            position=row.position
        )
        for row in condition_rows
    ]

    return StepRouteRead(
        id=route_row.id,
        form_id=route_row.form_id,
        source_step_id=route_row.source_step_id,
        target_step_id=route_row.target_step_id,
        priority=route_row.priority,
        description=route_row.description,
        scenario_description=route_row.scenario_description,
        logic_op=route_row.logic_op,
        condition_group_id=route_row.condition_group_id,
        conditions=conditions
    )


async def _fetch_routes_for_step(form_id: int, step_id: int) -> List[StepRouteRead]:
    query_ids = """
        SELECT id
        FROM step_transitions
        WHERE form_id = :form_id AND source_step_id = :step_id
        ORDER BY priority, id
    """
    rows = await database.fetch_all(query_ids, {"form_id": form_id, "step_id": step_id})
    result: List[StepRouteRead] = []
    for row in rows:
        result.append(await _fetch_route(row.id, form_id))
    return result


# ====================================================================
# 3. PYDANTIC МОДЕЛИ ДЛЯ "АДМИНКИ" (CRUD)
# ====================================================================

# --- Формы (Forms) ---
class FormBase(BaseModel):
    code: str
    title: str
    description: Optional[str] = None


class FormCreate(FormBase): pass


class FormRead(FormBase):
    id: int
    is_active: bool
    start_step_id: Optional[int] = None
    created_at: Any  # datetime


# --- Шаги (Steps) ---
class StepBase(BaseModel):
    code: str
    title: str
    step_type_code: str  # 'questionnaire', 'upload', etc.
    sort_order: int = 100
    is_terminal: bool = False


class StepCreate(StepBase): pass


class StepRead(StepBase):
    id: int
    form_id: int


class StepUpdate(BaseModel):
    title: Optional[str] = None
    step_type_code: Optional[str] = None
    sort_order: Optional[int] = None
    is_terminal: Optional[bool] = None
    is_start: Optional[bool] = None


# --- Справочники (Dictionaries) <--- НОВОЕ
class DictionaryValueCreate(BaseModel):
    value_code: str
    value_label: str
    sort_order: int = 100


class DictionaryBase(BaseModel):
    code: str
    title: str


class DictionaryCreate(DictionaryBase):
    values: List[DictionaryValueCreate] = []


class DictionaryRead(DictionaryBase):
    id: int
    values: List[DictionaryValueCreate] = []


# --- Поля (Fields) ---
class FieldOptionCreate(BaseModel):
    value_code: str
    value_label: str
    sort_order: int = 100


class FieldBase(BaseModel):
    code: str
    title: str
    data_type_code: str
    input_type_code: str
    is_required: bool = False
    sort_order: int = 100


class FieldCreate(FieldBase):
    # Поле может иметь ЛИБО локальные опции, ЛИБО ссылку на справочник
    options: Optional[List[FieldOptionCreate]] = None
    dictionary_code: Optional[str] = None

    @model_validator(mode='before')
    def check_options_or_dictionary(cls, values):
        options = values.get('options')
        dictionary_code = values.get('dictionary_code')
        input_type = values.get('input_type_code')

        if input_type in ('select', 'multiselect'):
            if options is not None and dictionary_code is not None:
                raise ValueError("Поле не может иметь 'options' и 'dictionary_code' одновременно")
            if options is None and dictionary_code is None:
                raise ValueError("Поля 'select' и 'multiselect' должны иметь 'options' или 'dictionary_code'")
        else:
            if options is not None or dictionary_code is not None:
                raise ValueError(f"Поле с типом '{input_type}' не может иметь 'options' или 'dictionary_code'")

        return values


class FieldRead(FieldBase):
    id: int
    step_id: int
    # UI всегда получает 'options', независимо от источника (локальный или глобальный)
    options: List[FieldOptionCreate] = []
    dictionary_code: Optional[str] = None  # Для админки, чтобы знала что привязано


# --- Условия и Переходы (Conditions & Transitions) ---
class ConditionBase(BaseModel):
    field_code: str
    op_code: str
    value_text: Optional[str] = None
    value_num: Optional[float] = None
    value_bool: Optional[bool] = None
    value_date: Optional[str] = None
    option_code: Optional[str] = None
    rhs_field_code: Optional[str] = None
    position: int = 100


class ConditionCreate(ConditionBase):
    pass


class ConditionRead(ConditionBase):
    id: int
    field_title: str
    op_title: str


class StepRouteBase(BaseModel):
    target_step_id: int
    priority: int = 100
    description: Optional[str] = None
    scenario_description: Optional[str] = None
    logic_op: str = "AND"
    conditions: List[ConditionCreate] = []


class StepRouteCreate(StepRouteBase):
    pass


class StepRouteUpdate(StepRouteBase):
    pass


class StepRouteRead(StepRouteBase):
    id: int
    form_id: int
    source_step_id: int
    condition_group_id: int
    conditions: List[ConditionRead]


# ====================================================================
# 4. API ЭНДПОИНТЫ "АДМИНКИ" (CRUD)
# ====================================================================

# --- CRUD для Форм (Forms) ---
@app.post("/admin/forms", response_model=FormRead, tags=["Admin - Forms"])
async def create_form(form: FormCreate):
    """Создает новую пустую форму (анкету)."""
    query = "INSERT INTO forms (code, title, description) VALUES (:code, :title, :description) RETURNING *"
    try:
        new_form = await database.fetch_one(query, form.dict())
        return new_form
    except sqlite3.IntegrityError as e:
        raise HTTPException(status_code=400, detail=f"Форма с кодом '{form.code}' уже существует. {e}")


@app.get("/admin/forms", response_model=List[FormRead], tags=["Admin - Forms"])
async def get_forms_list():
    query = "SELECT * FROM forms WHERE is_active = TRUE"
    return await database.fetch_all(query)


@app.get("/admin/forms/{form_id}", response_model=FormRead, tags=["Admin - Forms"])
async def get_form_details(form_id: int):
    query = "SELECT * FROM forms WHERE id = :form_id"
    form = await database.fetch_one(query, {"form_id": form_id})
    if not form: raise HTTPException(status_code=404, detail="Форма не найдена")
    return form


# --- CRUD для Шагов (Steps) ---
@app.post("/admin/forms/{form_id}/steps", response_model=StepRead, tags=["Admin - Steps"])
async def create_step(form_id: int, step: StepCreate):
    step_type_id = await _get_id_by_code("step_types", step.step_type_code)
    query = """
        INSERT INTO form_steps (form_id, step_type_id, code, title, sort_order, is_terminal)
        VALUES (:form_id, :step_type_id, :code, :title, :sort_order, :is_terminal)
        RETURNING *
    """
    values = step.dict()
    values["form_id"] = form_id
    values["step_type_id"] = step_type_id
    del values["step_type_code"]

    try:
        new_step = await database.fetch_one(query, values)
        await database.execute(
            "UPDATE forms SET start_step_id = :step_id WHERE id = :form_id AND start_step_id IS NULL",
            {"step_id": new_step.id, "form_id": form_id}
        )
        return await _fetch_step(new_step.id)
    except sqlite3.IntegrityError as e:
        raise HTTPException(status_code=400, detail=f"Шаг с кодом '{step.code}' уже существует в этой форме. {e}")


@app.get("/admin/forms/{form_id}/steps", response_model=List[StepRead], tags=["Admin - Steps"])
async def get_form_steps(form_id: int):
    query = """
        SELECT s.id
        FROM form_steps s
        WHERE s.form_id = :form_id
        ORDER BY s.sort_order, s.id
    """
    rows = await database.fetch_all(query, {"form_id": form_id})
    result: List[StepRead] = []
    for row in rows:
        result.append(await _fetch_step(row.id))
    return result


@app.put("/admin/forms/{form_id}/steps/{step_id}", response_model=StepRead, tags=["Admin - Steps"])
async def update_step(form_id: int, step_id: int, data: StepUpdate):
    await _ensure_step_in_form(form_id, step_id)

    update_parts = []
    values: Dict[str, Any] = {"form_id": form_id, "step_id": step_id}

    if data.title is not None:
        update_parts.append("title = :title")
        values["title"] = data.title
    if data.sort_order is not None:
        update_parts.append("sort_order = :sort_order")
        values["sort_order"] = data.sort_order
    if data.is_terminal is not None:
        update_parts.append("is_terminal = :is_terminal")
        values["is_terminal"] = data.is_terminal
    if data.step_type_code is not None:
        step_type_id = await _get_id_by_code("step_types", data.step_type_code)
        update_parts.append("step_type_id = :step_type_id")
        values["step_type_id"] = step_type_id

    if update_parts:
        query = "UPDATE form_steps SET " + ", ".join(update_parts) + " WHERE id = :step_id AND form_id = :form_id"
        await database.execute(query, values)

    if data.is_start is not None:
        if data.is_start:
            await database.execute(
                "UPDATE forms SET start_step_id = :step_id WHERE id = :form_id",
                {"step_id": step_id, "form_id": form_id}
            )
        else:
            await database.execute(
                "UPDATE forms SET start_step_id = NULL WHERE id = :form_id AND start_step_id = :step_id",
                {"form_id": form_id, "step_id": step_id}
            )

    return await _fetch_step(step_id)


# --- CRUD для Справочников (Dictionaries) <--- НОВОЕ ---
@app.post("/admin/dictionaries", response_model=DictionaryRead, status_code=status.HTTP_201_CREATED,
          tags=["Admin - Dictionaries"])
async def create_dictionary(dictionary: DictionaryCreate):
    """Создает новый глобальный справочник и все его значения."""
    async with database.transaction():
        query_dict = "INSERT INTO dictionaries (code, title) VALUES (:code, :title) RETURNING id"
        try:
            dict_id = await database.fetch_val(query=query_dict,
                                               values={"code": dictionary.code, "title": dictionary.title})
        except sqlite3.IntegrityError:
            raise HTTPException(status_code=400, detail=f"Справочник с кодом '{dictionary.code}' уже существует")

        query_value = """
            INSERT INTO dictionary_values (dictionary_id, value_code, value_label, sort_order)
            VALUES (:dict_id, :value_code, :value_label, :sort_order)
        """
        for value in dictionary.values:
            await database.execute(query_value, {**value.dict(), "dict_id": dict_id})

    return DictionaryRead(id=dict_id, **dictionary.dict())


@app.get("/admin/dictionaries", response_model=List[DictionaryRead], tags=["Admin - Dictionaries"])
async def get_dictionaries_list():
    """Получает список всех справочников с их значениями."""
    query_dicts = "SELECT * FROM dictionaries"
    dictionaries = await database.fetch_all(query_dicts)

    result = []
    for d in dictionaries:
        query_values = "SELECT value_code, value_label, sort_order FROM dictionary_values WHERE dictionary_id = :id ORDER BY sort_order"
        values = await database.fetch_all(query_values, {"id": d.id})
        result.append(
            DictionaryRead(id=d.id, code=d.code, title=d.title, values=[DictionaryValueCreate(**v) for v in values]))

    return result


# --- CRUD для Полей (Fields) ---
@app.post("/admin/steps/{step_id}/fields", response_model=FieldRead, tags=["Admin - Fields"])
async def create_field(step_id: int, field: FieldCreate):
    """
    Создает новое поле.
    Принимает ЛИБО 'options' (локальные), ЛИБО 'dictionary_code' (глобальные).
    """
    data_type_id = await _get_id_by_code("field_data_types", field.data_type_code)
    input_type_id = await _get_id_by_code("field_input_types", field.input_type_code)

    dictionary_id = None
    created_options = []

    # <--- ОБНОВЛЕННАЯ ЛОГИКА
    if field.dictionary_code:
        dictionary_id = await _get_id_by_code("dictionaries", field.dictionary_code)
    elif field.options:
        created_options = field.options
    # --->

    async with database.transaction():
        query_field = """
            INSERT INTO step_fields (step_id, code, title, data_type_id, input_type_id, dictionary_id, is_required, sort_order)
            VALUES (:step_id, :code, :title, :data_type_id, :input_type_id, :dictionary_id, :is_required, :sort_order)
            RETURNING id
        """
        values = field.dict()
        values["step_id"] = step_id
        values["data_type_id"] = data_type_id
        values["input_type_id"] = input_type_id
        values["dictionary_id"] = dictionary_id  # <--- НОВОЕ

        del values["options"]
        del values["dictionary_code"]
        del values["data_type_code"]
        del values["input_type_code"]

        try:
            field_id = await database.fetch_val(query=query_field, values=values)
        except sqlite3.IntegrityError as e:
            raise HTTPException(status_code=400, detail=f"Поле с кодом '{field.code}' уже существует на этом шаге. {e}")

        # Если были переданы ЛОКАЛЬНЫЕ опции, создаем их
        if created_options:
            query_option = """
                INSERT INTO field_options (field_id, value_code, value_label, sort_order)
                VALUES (:field_id, :value_code, :value_label, :sort_order)
            """
            for opt in created_options:
                await database.execute(query_option, {**opt.dict(), "field_id": field_id})

    # <--- ОБНОВЛЕННАЯ ЛОГИКА
    # Теперь нужно прочитать опции, чтобы вернуть их в FieldRead
    if dictionary_id:
        query_read_opts = "SELECT value_code, value_label, sort_order FROM dictionary_values WHERE dictionary_id = :id"
        options_rows = await database.fetch_all(query_read_opts, {"id": dictionary_id})
        created_options = [FieldOptionCreate(**row) for row in options_rows]
    # --->

    return FieldRead(id=field_id, step_id=step_id, **field.dict(), options=created_options)


@app.get("/admin/steps/{step_id}/fields", response_model=List[FieldRead], tags=["Admin - Fields"])
async def get_step_fields(step_id: int):
    """
    Получает все поля для шага.
    Корректно отдает 'options' из 'dictionaries' ИЛИ 'field_options'.
    """
    # <--- ОБНОВЛЕННЫЙ ЗАПРОС
    query = """
        SELECT 
            f.*, 
            dt.code AS data_type_code, 
            it.code AS input_type_code,
            d.code AS dictionary_code
        FROM step_fields f
        JOIN field_data_types dt ON f.data_type_id = dt.id
        JOIN field_input_types it ON f.input_type_id = it.id
        LEFT JOIN dictionaries d ON f.dictionary_id = d.id
        WHERE f.step_id = :step_id
        ORDER BY f.sort_order
    """
    fields_rows = await database.fetch_all(query, {"step_id": step_id})

    result = []
    for field in fields_rows:
        options = []
        # <--- ОБНОВЛЕННАЯ ЛОГИКА
        if field.input_type_code in ('select', 'multiselect'):
            if field.dictionary_id:
                # Берем опции из ГЛОБАЛЬНОГО справочника
                query_options = "SELECT value_code, value_label, sort_order FROM dictionary_values WHERE dictionary_id = :id ORDER BY sort_order"
                options_rows = await database.fetch_all(query_options, {"id": field.dictionary_id})
                options = [FieldOptionCreate(**row) for row in options_rows]
            else:
                # Берем опции из ЛОКАЛЬНЫХ
                query_options = "SELECT value_code, value_label, sort_order FROM field_options WHERE field_id = :id ORDER BY sort_order"
                options_rows = await database.fetch_all(query_options, {"id": field.id})
                options = [FieldOptionCreate(**row) for row in options_rows]

        result.append(FieldRead(**field, options=options))

    return result


# --- CRUD для маршрутов между шагами (Step Routes) ---
@app.get(
    "/admin/forms/{form_id}/steps/{step_id}/routes",
    response_model=List[StepRouteRead],
    tags=["Admin - Logic"]
)
async def list_step_routes(form_id: int, step_id: int):
    await _ensure_step_in_form(form_id, step_id)
    return await _fetch_routes_for_step(form_id, step_id)


@app.post(
    "/admin/forms/{form_id}/steps/{step_id}/routes",
    response_model=StepRouteRead,
    status_code=status.HTTP_201_CREATED,
    tags=["Admin - Logic"]
)
async def create_step_route(form_id: int, step_id: int, route: StepRouteCreate):
    await _ensure_step_in_form(form_id, step_id)
    await _ensure_step_in_form(form_id, route.target_step_id)

    async with database.transaction():
        group_id = await database.fetch_val(
            "INSERT INTO condition_groups (form_id, logic_op, description) VALUES (:form_id, :logic_op, :description) RETURNING id",
            {
                "form_id": form_id,
                "logic_op": route.logic_op,
                "description": route.scenario_description or "Transition group"
            }
        )

        for cond in route.conditions:
            field_id = await _get_field_id_by_code(form_id, cond.field_code)
            op_id = await _get_id_by_code("compare_ops", cond.op_code)
            rhs_field_id = None
            if cond.rhs_field_code:
                rhs_field_id = await _get_field_id_by_code(form_id, cond.rhs_field_code)

            query_cond = """
                INSERT INTO conditions (
                    group_id, field_id, op_id, value_text, value_num, value_bool, value_date,
                    option_code, rhs_field_id, position
                )
                VALUES (
                    :gid, :fid, :opid, :v_txt, :v_num, :v_bool, :v_date,
                    :v_opt, :rhs_id, :position
                )
            """
            await database.execute(query_cond, {
                "gid": group_id,
                "fid": field_id,
                "opid": op_id,
                "v_txt": cond.value_text,
                "v_num": cond.value_num,
                "v_bool": cond.value_bool,
                "v_date": cond.value_date,
                "v_opt": cond.option_code,
                "rhs_id": rhs_field_id,
                "position": cond.position,
            })

        route_id = await database.fetch_val(
            """
                INSERT INTO step_transitions (
                    form_id, source_step_id, target_step_id, condition_group_id, priority, description
                )
                VALUES (:form_id, :source_id, :target_id, :group_id, :priority, :description)
                RETURNING id
            """,
            {
                "form_id": form_id,
                "source_id": step_id,
                "target_id": route.target_step_id,
                "group_id": group_id,
                "priority": route.priority,
                "description": route.description,
            }
        )

    return await _fetch_route(route_id, form_id)


@app.get(
    "/admin/forms/{form_id}/routes/{route_id}",
    response_model=StepRouteRead,
    tags=["Admin - Logic"]
)
async def get_step_route(form_id: int, route_id: int):
    return await _fetch_route(route_id, form_id)


@app.put(
    "/admin/forms/{form_id}/routes/{route_id}",
    response_model=StepRouteRead,
    tags=["Admin - Logic"]
)
async def update_step_route(form_id: int, route_id: int, route: StepRouteUpdate):
    route_row = await database.fetch_one(
        "SELECT condition_group_id, source_step_id FROM step_transitions WHERE id = :id AND form_id = :form_id",
        {"id": route_id, "form_id": form_id}
    )
    if not route_row:
        raise HTTPException(status_code=404, detail=f"Переход {route_id} не найден в форме {form_id}")

    await _ensure_step_in_form(form_id, route.target_step_id)

    async with database.transaction():
        await database.execute(
            "UPDATE step_transitions SET target_step_id = :target, priority = :priority, description = :description WHERE id = :id",
            {
                "target": route.target_step_id,
                "priority": route.priority,
                "description": route.description,
                "id": route_id,
            }
        )

        await database.execute(
            "UPDATE condition_groups SET logic_op = :logic_op, description = :description WHERE id = :id",
            {
                "logic_op": route.logic_op,
                "description": route.scenario_description,
                "id": route_row.condition_group_id,
            }
        )

        await database.execute(
            "DELETE FROM conditions WHERE group_id = :group_id",
            {"group_id": route_row.condition_group_id}
        )

        for cond in route.conditions:
            field_id = await _get_field_id_by_code(form_id, cond.field_code)
            op_id = await _get_id_by_code("compare_ops", cond.op_code)
            rhs_field_id = None
            if cond.rhs_field_code:
                rhs_field_id = await _get_field_id_by_code(form_id, cond.rhs_field_code)

            await database.execute(
                """
                    INSERT INTO conditions (
                        group_id, field_id, op_id, value_text, value_num, value_bool, value_date,
                        option_code, rhs_field_id, position
                    )
                    VALUES (
                        :gid, :fid, :opid, :v_txt, :v_num, :v_bool, :v_date,
                        :v_opt, :rhs_id, :position
                    )
                """,
                {
                    "gid": route_row.condition_group_id,
                    "fid": field_id,
                    "opid": op_id,
                    "v_txt": cond.value_text,
                    "v_num": cond.value_num,
                    "v_bool": cond.value_bool,
                    "v_date": cond.value_date,
                    "v_opt": cond.option_code,
                    "rhs_id": rhs_field_id,
                    "position": cond.position,
                }
            )

    return await _fetch_route(route_id, form_id)


@app.get(
    "/admin/forms/{form_id}/steps/{step_id}/graph",
    tags=["Admin - Logic"]
)
async def get_step_graph(form_id: int, step_id: int):
    await _ensure_step_in_form(form_id, step_id)

    form = await database.fetch_one(
        "SELECT start_step_id FROM forms WHERE id = :form_id",
        {"form_id": form_id}
    )
    if not form:
        raise HTTPException(status_code=404, detail=f"Форма {form_id} не найдена")

    steps = await database.fetch_all(
        """
            SELECT id, code, title, sort_order, is_terminal
            FROM form_steps
            WHERE form_id = :form_id
            ORDER BY sort_order, id
        """,
        {"form_id": form_id}
    )

    routes = await database.fetch_all(
        """
            SELECT t.id, t.source_step_id, t.target_step_id, t.priority,
                   t.description, g.logic_op, g.description AS scenario_description
            FROM step_transitions t
            JOIN condition_groups g ON t.condition_group_id = g.id
            WHERE t.form_id = :form_id
            ORDER BY t.priority, t.id
        """,
        {"form_id": form_id}
    )

    return {
        "focus_step_id": step_id,
        "start_step_id": form.start_step_id,
        "steps": [
            {
                "id": row.id,
                "code": row.code,
                "title": row.title,
                "sort_order": row.sort_order,
                "is_terminal": bool(row.is_terminal),
                "is_start": row.id == form.start_step_id,
                "is_focus": row.id == step_id,
            }
            for row in steps
        ],
        "routes": [
            {
                "id": row.id,
                "source_step_id": row.source_step_id,
                "target_step_id": row.target_step_id,
                "priority": row.priority,
                "description": row.description,
                "scenario_description": row.scenario_description,
                "logic_op": row.logic_op,
            }
            for row in routes
        ],
    }


# TODO:
# - PUT/DELETE эндпоинты для админки
# - CRUD для Visibility Rules (по аналогии с Transitions)

# ====================================================================
# 5. PYDANTIC МОДЕЛИ ДЛЯ "РАНТАЙМА"
# ====================================================================

class StartFormRequest(BaseModel):
    user_id: int
    form_id: int


class UserFieldOption(BaseModel):  # Идентична FieldOptionCreate, но семантически разделяем
    value_code: str
    value_label: str
    # sort_order не нужен фронту при прохождении, только при рендере


class UserStepField(BaseModel):
    id: int
    code: str
    title: str
    input_type: str
    is_required: bool
    options: List[UserFieldOption]  # UI просто получает список


class StepResponse(BaseModel):
    instance_id: int
    step_id: int
    step_code: str
    step_title: str
    is_terminal: bool
    fields: List[UserStepField]
    values: Dict[str, Any]
    current_step_code: Optional[str] = None
    completed_steps: List[str] = Field(default_factory=list)
    available_steps: List[str] = Field(default_factory=list)


class Answer(BaseModel):
    field_code: str
    value: Any


class SubmitStepRequest(BaseModel):
    answers: List[Answer]


class SubmitStepResponse(BaseModel):
    instance_id: int
    next_step_id: Optional[int]
    next_step_code: Optional[str]
    is_complete: bool
    completed_steps: List[str] = Field(default_factory=list)
    available_steps: List[str] = Field(default_factory=list)


# ====================================================================
# 6. HELPER-ФУНКЦИИ ДЛЯ "РАНТАЙМА"
# ====================================================================

form_sessions: Dict[int, Dict[str, Any]] = {}


async def _get_step_codes(step_ids: List[int]) -> Dict[int, str]:
    """Возвращает словарь {step_id: step_code}."""
    if not step_ids:
        return {}

    placeholders = ", ".join(f":sid_{idx}" for idx in range(len(step_ids)))
    query = f"SELECT id, code FROM form_steps WHERE id IN ({placeholders})"
    values = {f"sid_{idx}": step_id for idx, step_id in enumerate(step_ids)}

    rows = await database.fetch_all(query, values)
    return {row.id: row.code for row in rows}


async def _get_step_id_by_code(form_id: int, step_code: str) -> int:
    row = await database.fetch_one(
        "SELECT id FROM form_steps WHERE form_id = :fid AND code = :code",
        {"fid": form_id, "code": step_code}
    )
    if not row:
        raise HTTPException(status_code=404, detail=f"Шаг с кодом '{step_code}' не найден")
    return row.id


async def _get_valid_forward_steps(instance_id: int, source_step_id: int) -> List[int]:
    """Возвращает список доступных шагов для перехода вперед."""
    query = (
        "SELECT target_step_id, condition_group_id "
        "FROM step_transitions WHERE source_step_id = :id ORDER BY priority"
    )
    transitions = await database.fetch_all(query, {"id": source_step_id})
    if not transitions:
        return []

    all_answers = await _get_all_answers(instance_id)

    allowed: List[int] = []
    for trans in transitions:
        if await _evaluate_group(trans.condition_group_id, all_answers):
            allowed.append(trans.target_step_id)
    return allowed


async def _get_navigation_summary(instance_id: int, current_step_id: Optional[int]) -> Dict[str, Any]:
    """Строит список завершенных и доступных шагов для UI."""
    completed_rows = await database.fetch_all(
        """
        SELECT fs.id, fs.code
        FROM instance_steps ist
        JOIN form_steps fs ON ist.step_id = fs.id
        WHERE ist.instance_id = :instance_id AND ist.status_code = 'completed'
        ORDER BY ist.entered_at
        """,
        {"instance_id": instance_id}
    )

    completed_codes = [row.code for row in completed_rows]
    available_codes: List[str] = list(dict.fromkeys(completed_codes))
    current_code: Optional[str] = None

    if current_step_id:
        current_row = await database.fetch_one(
            "SELECT code FROM form_steps WHERE id = :id",
            {"id": current_step_id}
        )
        if current_row:
            current_code = current_row.code
            if current_code not in available_codes:
                available_codes.append(current_code)

        forward_ids = await _get_valid_forward_steps(instance_id, current_step_id)
        if forward_ids:
            forward_codes_map = await _get_step_codes(forward_ids)
            for step_id in forward_ids:
                code = forward_codes_map.get(step_id)
                if code and code not in available_codes:
                    available_codes.append(code)

    return {
        "current_step_code": current_code,
        "completed_steps": completed_codes,
        "available_steps": available_codes,
    }


async def _sync_session_state(instance_id: int) -> Dict[str, Any]:
    """Обновляет кеш состояния сессии и возвращает его."""
    session_row = await database.fetch_one(
        "SELECT form_id, current_step_id FROM form_instances WHERE id = :id",
        {"id": instance_id}
    )
    if not session_row:
        raise HTTPException(status_code=404, detail="Сессия формы не найдена")

    form_row = await database.fetch_one(
        "SELECT code FROM forms WHERE id = :id",
        {"id": session_row.form_id}
    )
    if not form_row:
        raise HTTPException(status_code=404, detail="Форма для текущей сессии не найдена")

    navigation = await _get_navigation_summary(instance_id, session_row.current_step_id)

    state = {
        "form_id": session_row.form_id,
        "form_code": form_row.code,
        "current_step_id": session_row.current_step_id,
        "current_step_code": navigation.get("current_step_code"),
        "completed_steps": navigation.get("completed_steps", []),
        "available_steps": navigation.get("available_steps", []),
    }
    form_sessions[instance_id] = state
    return state


async def _validate_navigation(instance_id: int, current_step_id: Optional[int], target_step_id: int) -> None:
    """Проверяет, что переход к целевому шагу разрешен."""
    completed_rows = await database.fetch_all(
        "SELECT step_id FROM instance_steps WHERE instance_id = :id AND status_code = 'completed'",
        {"id": instance_id}
    )
    allowed_ids = {row.step_id for row in completed_rows}

    if current_step_id:
        allowed_ids.add(current_step_id)
        forward_ids = await _get_valid_forward_steps(instance_id, current_step_id)
        allowed_ids.update(forward_ids)

    if target_step_id not in allowed_ids:
        raise HTTPException(status_code=403, detail="Переход к указанному шагу недоступен")


async def _get_session_for_form(form_code: str, session_id: int) -> Tuple[int, Dict[str, Any]]:
    """Возвращает form_id и состояние сессии, проверяя принадлежность форме."""
    form_id = await _get_id_by_code("forms", form_code)
    session_row = await database.fetch_one(
        "SELECT form_id FROM form_instances WHERE id = :sid",
        {"sid": session_id}
    )
    if not session_row or session_row.form_id != form_id:
        raise HTTPException(status_code=404, detail="Сессия формы не найдена")

    state = await _sync_session_state(session_id)
    return form_id, state


async def _get_step_details(instance_id: int, step_id: int, *, current_step_id: Optional[int] = None) -> StepResponse:
    """
    Собирает всю информацию о шаге.
    Корректно отдает 'options' из 'dictionaries' ИЛИ 'field_options'.
    """
    step_row = await database.fetch_one(
        "SELECT id, code, title, is_terminal FROM form_steps WHERE id = :step_id",
        {"step_id": step_id}
    )
    if not step_row:
        raise HTTPException(status_code=404, detail=f"Шаг {step_id} не найден")

    # <--- ОБНОВЛЕННЫЙ ЗАПРОС
    query_fields = """
        SELECT 
            f.id, f.code, f.title, 
            ft.code AS input_type, 
            f.is_required,
            f.dictionary_id
        FROM step_fields f 
        JOIN field_input_types ft ON f.input_type_id = ft.id
        WHERE f.step_id = :step_id 
        ORDER BY f.sort_order
    """
    fields_rows = await database.fetch_all(query_fields, {"step_id": step_id})

    query_values = """
        SELECT sf.code, a.value_text, a.value_num, a.value_bool, a.value_date
        FROM instance_answers a JOIN step_fields sf ON a.field_id = sf.id
        WHERE a.instance_id = :instance_id AND sf.step_id = :step_id
    """
    values_rows = await database.fetch_all(query_values, {"instance_id": instance_id, "step_id": step_id})

    values_dict: Dict[str, Any] = {
        row.code: next((v for v in (row.value_bool, row.value_text, row.value_num, row.value_date) if v is not None),
                       None)
        for row in values_rows
    }

    fields_list = []
    for field in fields_rows:
        options = []
        # <--- ОБНОВЛЕННАЯ ЛОГИКА
        if field.input_type in ('select', 'multiselect'):
            if field.dictionary_id:
                # Глобальный справочник
                query_options = "SELECT value_code, value_label FROM dictionary_values WHERE dictionary_id = :id ORDER BY sort_order"
                options_rows = await database.fetch_all(query_options, {"id": field.dictionary_id})
                options = [UserFieldOption(**row) for row in options_rows]
            else:
                # Локальные опции
                query_options = "SELECT value_code, value_label FROM field_options WHERE field_id = :id ORDER BY sort_order"
                options_rows = await database.fetch_all(query_options, {"id": field.id})
                options = [UserFieldOption(**row) for row in options_rows]

        fields_list.append(UserStepField(
            id=field.id, code=field.code, title=field.title,
            input_type=field.input_type, is_required=field.is_required, options=options
        ))

    active_step_id = current_step_id if current_step_id is not None else step_id
    navigation = await _get_navigation_summary(instance_id, active_step_id)

    return StepResponse(
        instance_id=instance_id,
        step_id=step_id,
        step_code=step_row.code,
        step_title=step_row.title,
        is_terminal=step_row.is_terminal,
        fields=fields_list,
        values=values_dict,
        current_step_code=navigation.get("current_step_code"),
        completed_steps=navigation.get("completed_steps", []),
        available_steps=navigation.get("available_steps", []),
    )


async def _save_answers(instance_id: int, step_id: int, answers: List[Answer]):
    """Сохраняет ответы пользователя (UPSERT)."""
    query_fields = """
        SELECT f.id, f.code, dt.code AS data_type
        FROM step_fields f JOIN field_data_types dt ON f.data_type_id = dt.id
        WHERE f.step_id = :step_id
    """
    fields_info = await database.fetch_all(query_fields, {"step_id": step_id})
    field_map = {row.code: (row.id, row.data_type) for row in fields_info}

    async with database.transaction():
        for ans in answers:
            if ans.field_code not in field_map:
                continue

            field_id, data_type = field_map[ans.field_code]
            raw_value = ans.value

            value_text = None
            value_num = None
            value_bool = None
            value_date = None

            if data_type == 'boolean':
                value_bool = bool(raw_value) if raw_value is not None else None
            elif data_type in ('integer', 'decimal'):
                value_num = float(raw_value) if raw_value is not None else None
            elif data_type in ('date', 'datetime'):
                value_date = raw_value
            else:
                value_text = str(raw_value) if raw_value is not None else None

            query_ins = """
                INSERT INTO instance_answers (instance_id, field_id, value_text, value_num, value_bool, value_date, updated_at)
                VALUES (:id, :fid, :v_text, :v_num, :v_bool, :v_date, CURRENT_TIMESTAMP)
                ON CONFLICT(instance_id, field_id) DO UPDATE SET
                    value_text = excluded.value_text,
                    value_num = excluded.value_num,
                    value_bool = excluded.value_bool,
                    value_date = excluded.value_date,
                    updated_at = CURRENT_TIMESTAMP
            """
            await database.execute(query_ins, {
                "id": instance_id,
                "fid": field_id,
                "v_text": value_text,
                "v_num": value_num,
                "v_bool": value_bool,
                "v_date": value_date,
            })

            # TODO: Добавить обработку multiselect (instance_answers_multi)


async def _check_condition(actual_value: Any, op_code: str, expected_value: Any) -> bool:
    """Проверяет одно атомарное условие."""
    try:
        if op_code == 'is_true': return bool(actual_value) == True
        if op_code == 'is_false': return bool(actual_value) == False
        if op_code == 'eq':
            if isinstance(expected_value, bool): return bool(actual_value) == expected_value
            if isinstance(expected_value, (int, float)): return float(actual_value) == float(expected_value)
            return str(actual_value) == str(expected_value)
        if op_code == 'ne': return not (await _check_condition(actual_value, 'eq', expected_value))
        if op_code == 'is_empty': return actual_value is None or actual_value == ""
        if op_code == 'not_empty': return actual_value is not None and actual_value != ""
    except (ValueError, TypeError):
        return False
    return False


async def _evaluate_group(group_id: int, all_answers: Dict[int, Any]) -> bool:
    """Вычисляет, истинна ли группа условий (AND/OR)."""
    group_row = await database.fetch_one("SELECT logic_op FROM condition_groups WHERE id = :id", {"id": group_id})
    if not group_row: return False

    conditions_query = """
        SELECT c.field_id, op.code AS op_code,
               c.value_text, c.value_num, c.value_bool, c.value_date, c.option_code
        FROM conditions c JOIN compare_ops op ON c.op_id = op.id
        WHERE c.group_id = :group_id ORDER BY c.position
    """
    conditions_rows = await database.fetch_all(conditions_query, {"group_id": group_id})
    if not conditions_rows: return True

    results = []
    for cond in conditions_rows:
        actual_value = all_answers.get(cond.field_id)
        expected_value = next(
            (v for v in (cond.value_bool, cond.value_text, cond.value_num, cond.value_date, cond.option_code) if
             v is not None), None)
        results.append(await _check_condition(actual_value, cond.op_code, expected_value))

    return all(results) if group_row.logic_op == 'AND' else any(results)


async def _get_all_answers(instance_id: int) -> Dict[int, Any]:
    """Собирает ВСЕ ответы для инстанса в словарь {field_id: value}."""
    query = "SELECT field_id, value_text, value_num, value_bool, value_date FROM instance_answers WHERE instance_id = :id"
    rows = await database.fetch_all(query, {"id": instance_id})

    answers_map: Dict[int, Any] = {
        row.field_id: next(
            (v for v in (row.value_bool, row.value_text, row.value_num, row.value_date) if v is not None), None)
        for row in rows
    }
    # TODO: Добавить instance_answers_multi
    return answers_map


async def _determine_next_step(instance_id: int, current_step_id: int) -> Optional[int]:
    """Определяет следующий шаг на основе правил перехода."""
    current_step = await database.fetch_one("SELECT is_terminal FROM form_steps WHERE id = :id",
                                            {"id": current_step_id})
    if current_step.is_terminal: return None

    valid_steps = await _get_valid_forward_steps(instance_id, current_step_id)
    return valid_steps[0] if valid_steps else None


async def _get_instance_status_id(code: str) -> int:
    row = await database.fetch_one("SELECT id FROM instance_statuses WHERE code = :code", {"code": code})
    if not row: raise ValueError(f"Статус '{code}' не найден в БД")
    return row.id


# ====================================================================
# 7. API ЭНДПОИНТЫ "РАНТАЙМА"
# ====================================================================

@app.post("/start", response_model=StepResponse, tags=["User - Runtime"])
async def start_form(data: StartFormRequest):
    """Начинает новую сессию анкетирования или возвращает текущую."""
    status_in_progress_id = await _get_instance_status_id('in_progress')

    query_find = "SELECT id, current_step_id FROM form_instances WHERE form_id = :fid AND user_id = :uid AND status_id = :sid LIMIT 1"
    instance = await database.fetch_one(query_find,
                                        {"fid": data.form_id, "uid": data.user_id, "sid": status_in_progress_id})

    if instance:
        state = await _sync_session_state(instance.id)
        return await _get_step_details(
            instance.id,
            instance.current_step_id,
            current_step_id=state.get("current_step_id")
        )

    form_row = await database.fetch_one("SELECT start_step_id FROM forms WHERE id = :id AND is_active = TRUE",
                                        {"id": data.form_id})
    if not form_row or not form_row.start_step_id:
        raise HTTPException(status_code=404, detail="Активная форма или ее стартовый шаг не найдены")

    start_step_id = form_row.start_step_id

    query_insert = """
        INSERT INTO form_instances (form_id, user_id, status_id, current_step_id, started_at, updated_at)
        VALUES (:fid, :uid, :sid, :step_id, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) RETURNING id
    """
    instance_id = await database.fetch_val(query=query_insert, values={
        "fid": data.form_id, "uid": data.user_id, "sid": status_in_progress_id, "step_id": start_step_id
    })

    query_log_step = "INSERT INTO instance_steps (instance_id, step_id, status_code, entered_at) VALUES (:id, :step_id, 'entered', CURRENT_TIMESTAMP)"
    await database.execute(query_log_step, {"id": instance_id, "step_id": start_step_id})

    state = await _sync_session_state(instance_id)
    return await _get_step_details(instance_id, start_step_id, current_step_id=state.get("current_step_id"))


@app.get("/instance/{instance_id}", response_model=StepResponse, tags=["User - Runtime"])
async def get_current_step(instance_id: int):
    """Получает текущий шаг для указанного инстанса анкеты."""
    instance = await database.fetch_one("SELECT current_step_id FROM form_instances WHERE id = :id",
                                        {"id": instance_id})
    if not instance or not instance.current_step_id:
        raise HTTPException(status_code=404, detail="Инстанс или текущий шаг не найдены")

    state = await _sync_session_state(instance_id)
    return await _get_step_details(
        instance_id,
        instance.current_step_id,
        current_step_id=state.get("current_step_id")
    )


@app.post("/instance/{instance_id}/submit", response_model=SubmitStepResponse, tags=["User - Runtime"])
async def submit_step(instance_id: int, data: SubmitStepRequest):
    """Принимает ответы на текущий шаг, сохраняет их и возвращает следующий шаг."""
    instance = await database.fetch_one("SELECT current_step_id FROM form_instances WHERE id = :id",
                                        {"id": instance_id})
    if not instance or not instance.current_step_id:
        raise HTTPException(status_code=404, detail="Инстанс не найден")

    current_step_id = instance.current_step_id

    await _save_answers(instance_id, current_step_id, data.answers)

    await database.execute(
        "UPDATE instance_steps SET status_code = 'completed', left_at = CURRENT_TIMESTAMP WHERE instance_id = :id AND step_id = :step_id",
        {"id": instance_id, "step_id": current_step_id})

    next_step_id = await _determine_next_step(instance_id, current_step_id)

    if next_step_id:
        await database.execute(
            "UPDATE form_instances SET current_step_id = :step_id, updated_at = CURRENT_TIMESTAMP WHERE id = :id",
            {"step_id": next_step_id, "id": instance_id})
        await database.execute(
            "INSERT INTO instance_steps (instance_id, step_id, status_code, entered_at) VALUES (:id, :step_id, 'entered', CURRENT_TIMESTAMP)",
            {"id": instance_id, "step_id": next_step_id})

        state = await _sync_session_state(instance_id)
        return SubmitStepResponse(
            instance_id=instance_id,
            next_step_id=next_step_id,
            next_step_code=state.get("current_step_code"),
            is_complete=False,
            completed_steps=state.get("completed_steps", []),
            available_steps=state.get("available_steps", []),
        )
    else:
        status_completed_id = await _get_instance_status_id('completed')
        await database.execute(
            "UPDATE form_instances SET status_id = :sid, current_step_id = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = :id",
            {"sid": status_completed_id, "id": instance_id})

        state = await _sync_session_state(instance_id)
        return SubmitStepResponse(
            instance_id=instance_id,
            next_step_id=None,
            next_step_code=None,
            is_complete=True,
            completed_steps=state.get("completed_steps", []),
            available_steps=state.get("available_steps", []),
        )


@app.get(
    "/runtime/forms/{form_code}/sessions/{session_id}/steps/{step_code}",
    response_model=StepResponse,
    tags=["User - Runtime"]
)
async def get_session_step(form_code: str, session_id: int, step_code: str):
    """Возвращает состояние конкретного шага в рамках сессии формы."""
    form_id, state = await _get_session_for_form(form_code, session_id)
    step_id = await _get_step_id_by_code(form_id, step_code)
    await _validate_navigation(session_id, state.get("current_step_id"), step_id)

    return await _get_step_details(session_id, step_id, current_step_id=state.get("current_step_id"))


@app.put(
    "/runtime/forms/{form_code}/sessions/{session_id}/steps/{step_code}",
    response_model=StepResponse,
    tags=["User - Runtime"]
)
async def update_session_step(form_code: str, session_id: int, step_code: str, data: SubmitStepRequest):
    """Сохраняет ответы для указанного шага, если переход разрешен."""
    form_id, state = await _get_session_for_form(form_code, session_id)
    step_id = await _get_step_id_by_code(form_id, step_code)
    await _validate_navigation(session_id, state.get("current_step_id"), step_id)

    await _save_answers(session_id, step_id, data.answers)

    updated_state = await _sync_session_state(session_id)
    return await _get_step_details(session_id, step_id, current_step_id=updated_state.get("current_step_id"))


@app.get("/", include_in_schema=False)
async def serve_index():
    if not INDEX_HTML.exists():
        # чтобы сразу понять, если файла нет/опечатка в имени
        raise HTTPException(status_code=404, detail="index.html not found in project root")
    # отдаём как статический файл
    return FileResponse(
        path=str(INDEX_HTML),
        media_type="text/html; charset=utf-8",
        headers={"Cache-Control": "no-store"}  # удобно в разработке
    )


# ====================================================================
# 8. ЗАПУСК СЕРВЕРА
# ====================================================================

if __name__ == "__main__":
    print("=" * 50)
    print("Сервер FastAPI готов.")
    print("Запустите миграцию: python migrate.py (если еще не сделали)")
    print("Запустите сервер: uvicorn main:app --reload")
    print("Документация API будет доступна по адресу: http://127.0.0.1:8000/docs")
    print("=" * 50)
    uvicorn.run(app, host="127.0.0.1", port=8000)