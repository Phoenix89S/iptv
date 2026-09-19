#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path
import shutil


# Папка, где находится этот Python-файл
BASE_DIR = Path(__file__).resolve().parent

# Исходная папка
SOURCE = BASE_DIR / "mega_iptv_output"

# Новая папка с префиксом _1
TARGET = BASE_DIR / "mega_iptv_output_1"


def copy_folder():
    print("=" * 70)
    print("COPY MEGA IPTV OUTPUT")
    print("=" * 70)

    print(f"Источник : {SOURCE}")
    print(f"Назначение: {TARGET}")
    print()

    # Проверяем источник
    if not SOURCE.exists():
        print(f"ERROR: папка не найдена: {SOURCE}")
        return

    if not SOURCE.is_dir():
        print(f"ERROR: это не папка: {SOURCE}")
        return

    # Создаём новую папку.
    # Исходная папка НЕ удаляется и НЕ изменяется.
    TARGET.mkdir(parents=True, exist_ok=True)

    copied = 0

    # Копируем всё содержимое
    for item in SOURCE.iterdir():

        destination = TARGET / item.name

        if item.is_dir():
            shutil.copytree(
                item,
                destination,
                dirs_exist_ok=True
            )
            print(f"[DIR ] {item.name}")
            copied += 1

        elif item.is_file():
            shutil.copy2(item, destination)
            print(f"[FILE] {item.name}")
            copied += 1

        elif item.is_symlink():
            print(f"[LINK] {item.name}")

    print()
    print("=" * 70)
    print("ГОТОВО")
    print("=" * 70)
    print(f"Скопировано объектов: {copied}")
    print(f"Источник : {SOURCE}")
    print(f"Копия    : {TARGET}")
    print()
    print("Исходная папка НЕ изменена.")


if __name__ == "__main__":
    copy_folder()