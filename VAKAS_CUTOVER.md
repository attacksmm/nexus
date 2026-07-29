# Переезд с Vakas Tools на Nexus

## 1. Установка без внешних изменений

1. Обновить `bizon-reports.zip` и установить `bizon-amocrm.zip`, `bizon-google-sheets.zip`, `getcourse-amocrm.zip`.
2. Убедиться, что `bizon-amocrm` и `bizon-google-sheets` запущены с включённым `dry_run`.
3. В `bizon-reports` проверить Bizon API token, Project ID `97242`, webhook secret и скопировать feed token.
4. Передать feed token в оба downstream-модуля через `NEXUS_BIZON_FEED_TOKEN` либо их защищённые настройки.
5. Указать существующий JSON-ключ через `GOOGLE_APPLICATION_CREDENTIALS`. Ключ не загружать в Nexus как ZIP/файл модуля.

## 2. Настройка и теневой период

1. В `bizon-amocrm` прочитать сделки `17954367` и `17947711` как технический пресет.
2. Для каждого типа вебинара создать явную связку на 60 или 80 минут. Ненастроенные посещения останутся в `pending_binding`.
3. В каждой связке выбрать целевую воронку/статус, обе воронки «Исходящие» для поиска, активных менеджеров, теги, поля и правила дублей PHONE → EMAIL.
4. В `getcourse-amocrm` прочитать `17954261` и `17169867`, затем сверить связки `created`, `partial`, `paid`.
5. Оставить forwarding Bizon → Vakas включённым. Сравнить несколько новых отчётов: число профилей/клиентов, минуты, ожидаемое действие, найденный дубль, выбранный менеджер и поля сделки.
6. Включить запись `bizon-google-sheets`, выполнить «Проверить таблицу» и убедиться, что создана новая вкладка `Bizon365 Nexus`. Старую вкладку не менять.

## 3. Переключение

1. Зафиксировать последний отчёт, который обработал Vakas.
2. Отключить forwarding на Vakas в webhook Bizon и только затем выключить `dry_run` в `bizon-amocrm`.
3. Shadow-события amoCRM не переигрывать. Новые события после переключения должны начинаться со следующего change ID.
4. Для GetCourse остановить старый Vakas-маршрут, затем включить связки/автосинхронизацию `getcourse-amocrm`. Существующие строки `getcourse_orders` остаются bootstrapped без backfill.
5. Проверить первые сделки, примечания, владельцев и строки Sheets вручную.

## 4. Rollback

1. Поставить связки `bizon-amocrm` и `getcourse-amocrm` на паузу или включить dry-run.
2. Вернуть прежние webhook/forward URL Vakas.
3. Не сбрасывать feed cursor и таблицы идемпотентности: уже успешные `attendance_key`, change ID и `order_id` повторно не отправлять.
4. После исправления повторять только события `failed`/`pending_binding`; shadow amoCRM остаётся терминальным.
