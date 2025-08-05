from __future__ import annotations

from typing import Callable, Awaitable
from datetime import datetime
import inspect
import asyncio


class Commander:
    """
    Иерархический реестр команд.
    Ключи хранятся в нижнем регистре. Поддерживаются как sync-, так и async-функции.
    """

    def __init__(self, logs: bool = True) -> None:
        self.tree: dict[str, dict] = {}
        self.logs = logs

    def show_tree(self, node: dict | None = None, depth: int = 0) -> str:
        if node is None:
            node = self.tree

        result_lines: list[str] = []

        # Сортируем для стабильного вывода
        for key in sorted(node.keys()):
            if key == "func":
                argspec = inspect.getfullargspec(node[key])
                argument_names = argspec.args
                args_str = ", ".join(map(str, argument_names))
                result_lines.append("  " * depth + f"-- {args_str}")
            else:
                result_lines.append("  " * depth + f"{depth + 1} {key}")
                result_lines.append(self.show_tree(node[key], depth + 1))

        return "\n".join(result_lines) if result_lines else ""

    def add_command(self, command: list[str], func: Callable[..., Awaitable | None]) -> None:
        """
        Регистрирует команду по списку токенов.
        Все токены приводятся к нижнему регистру.
        """
        current_level = self.tree
        for part in command:
            key = part.lower()
            if key not in current_level:
                current_level[key] = {}
            current_level = current_level[key]
        current_level["func"] = func

    def decode_str(self, prompt: str) -> tuple[list[str], list[str]]:
        """
        Разбор строки:
        - токены команды разделены пробелами
        - параметры передаются как `--value` (всё после `--` — значение параметра)
        Пример: "sim 5 --0.03 --0.06"
        """
        # допускаем множественные пробелы
        elements = [e for e in prompt.split(" ") if e]
        command: list[str] = []
        params: list[str] = []
        for el in elements:
            if el.startswith("--"):
                params.append(el[2:])
            else:
                command.append(el)
        return command, params

    async def exec_command(self, prompt: str) -> None:
        """
        Находит функцию по дереву и вызывает её, передавая параметры как позиционные.
        Если функция синхронная — выполняется в отдельном потоке через loop.run_in_executor.
        """
        command, params = self.decode_str(prompt.lower())
        current_level = self.tree
        for part in command:
            key = part.lower()
            if key in current_level:
                current_level = current_level[key]
            else:
                if self.logs:
                    print(f"{datetime.now()} Command not found: {' '.join(command)}")
                return

        func = current_level.get("func")
        if not func:
            if self.logs:
                print(f"{datetime.now()} No function assigned for command: {' '.join(command)}")
            return

        try:
            if inspect.iscoroutinefunction(func):
                await func(*params)
            else:
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, lambda: func(*params))
            if self.logs:
                print(f"{datetime.now()} Command successfully executed: {' '.join(command)}")
        except Exception as exc:
            if self.logs:
                print(f"{datetime.now()} Command execution failed: {' '.join(command)} -> {exc}")
