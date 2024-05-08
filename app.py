from pydantic import BaseModel
from choicesenum import ChoicesEnum

from dotenv import dotenv_values
import json
import bson

import asyncio
import logging
import datetime
from dateutil.relativedelta import relativedelta
from functools import lru_cache

from aiogram import Dispatcher, Bot
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import Message


help_msg = """Send me ISO-ed dataframe (dt_from AND dt_upto)
     and a group_type (ENUM: hour | day | month) 
     in a dict format to retrieve dataset.
    \nExample:
{
   "dt_from": "2022-09-01T00:00:00",
   "dt_upto": "2022-12-31T23:59:00",
   "group_type": "month"
}"""

TOKEN = dotenv_values(".env")["TOKEN"]
dispatcher = Dispatcher()

with open("sample_collection.bson", "rb") as file:
    bson_data = bson.decode_all(file.read())

sorted_documents = sorted(bson_data, key=lambda x: x["dt"])


class GroupEnum(ChoicesEnum):
    MONTH = "month"
    DAY = "day"
    HOUR = "hour"


class InputModel(BaseModel):
    """Модель валидации для входящих данных"""

    date_from: datetime.datetime
    date_upto: datetime.datetime
    group_type: GroupEnum

    def __init__(self, dt_from: str, dt_upto: str, group_type: str):
        super().__init__(
            date_from=datetime.datetime.fromisoformat(dt_from),
            date_upto=datetime.datetime.fromisoformat(dt_upto),
            group_type=GroupEnum(group_type)
        )


@dispatcher.message()
async def message_handler(message: Message) -> None:
    """Обработчик сообщений Telegram бота"""

    input_data: InputModel
    try:
        input_data = InputModel(**json.loads(message.text))
        await message.answer(get_dataset(input_data))
    except Exception as e:
        logging.error(f"{type(e).__name__} for msg \"{message.text}\": {str(e)}")
        await message.answer(help_msg)


def get_dataset(input_model: InputModel) -> str:
    """Формирование ответа по агрегации
    1. Находим индекс в отсортированной коллекции
    2. Агрегируем значения по "шагу" агрегации
    3. Применяем "расширение" для исходящего формата и возвращаем коллекцию"""

    start_index = get_index_by_date(input_model.date_from)
    group_type = str(input_model.group_type)
    date_anchor = sorted_documents[start_index]["dt"].__getattribute__(group_type)
    result = {
        "dataset": [sorted_documents[start_index]["value"]],
        "labels": [normalize_date_label(
            sorted_documents[start_index]["dt"],
            input_model,
        ).isoformat()]
    }

    for doc in sorted_documents[start_index + 1:]:
        if doc["dt"] > input_model.date_upto:
            return extend_dataset(result, input_model)
        if doc["dt"].__getattribute__(group_type) != date_anchor:
            result["dataset"].append(doc["value"])
            result["labels"].append(
                normalize_date_label(
                    doc["dt"],
                    input_model,
                ).isoformat()
            )
            date_anchor = doc["dt"].__getattribute__(group_type)
        else:
            result["dataset"][-1] += doc["value"]


def normalize_date_label(
    date_label: datetime.datetime,
    input_model: InputModel
) -> datetime.datetime:
    """Нормализация дат для временных меток по "шагу" агрегации"""

    order = {
        "second": 0, "minute": 0, "hour": 0, "day": 1, "month": 1
    }
    reset_params = {}
    for key, value in order.items():
        if key == input_model.group_type:
            break
        reset_params[key] = value
    date_label = date_label.replace(**reset_params)
    return date_label


def extend_dataset(existing_dataset: dict, input_model: InputModel) -> str:
    """Расширение датасета для "плавного" и "пошагового" отображения метрик"""

    group_type = input_model.group_type.value + "s"
    current_date = input_model.date_from
    date_upto = input_model.date_upto
    extended = {"dataset": [], "labels": []}

    while current_date <= date_upto:
        if normalize_date_label(current_date, input_model).isoformat() not in existing_dataset["labels"]:
            extended["dataset"].append(0)
            extended["labels"].append(current_date.isoformat())
        else:
            extended["dataset"].append(existing_dataset["dataset"].pop(0))
            extended["labels"].append(existing_dataset["labels"].pop(0))

        current_date += relativedelta(**{group_type: 1})

    return json.dumps(extended)


@lru_cache(maxsize=None)
def get_index_by_date(date: datetime.datetime) -> int:
    """Поиск индекса документа с подходящей датой"""

    left, right = 0, len(sorted_documents) - 1
    while left <= right:
        mid = (left + right) // 2
        if sorted_documents[mid]["dt"] >= date:
            if mid == 0 or sorted_documents[mid - 1]["dt"] < date:
                return mid
            else:
                right = mid - 1
        else:
            left = mid + 1


async def main():
    bot = Bot(
        token=TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )
    await dispatcher.start_polling(bot)


if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
    asyncio.run(main())
