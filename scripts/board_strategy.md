Для достижения оценки 10/10, необходимо устранить все выявленные проблемы. Разделим задачи на спринты, чтобы последовательно улучшать продукт.

### Спринт 1: Устранение пустых секций

1. **Digital-аудит (P4)**
   - Файл: `app/pipeline/step5_deep_analysis.py`
   - Задача: В строке, где формируется объект digital, добавить проверку и заполнение `social_accounts`.
   - Код: 
     ```python
     if not digital.get('social_accounts'):
         digital['social_accounts'] = fetch_social_accounts(company_url)
     ```
   - Ожидаемый результат: Секция digital-аудита будет заполнена.

2. **Продукты и услуги (P5)**
   - Файл: `app/pipeline/step5_deep_analysis.py`
   - Задача: Добавить генерацию поля `products`.
   - Код:
     ```python
     products = fetch_products(company_url)
     report_data['products'] = products
     ```
   - Ожидаемый результат: Секция "Продукты и услуги" будет заполнена.

3. **Фактчек (F1) и Верификация digital (F2)**
   - Файл: `app/pipeline/step2a_verify.py`
   - Задача: Заполнить поля `factcheck` и `digital_verification`.
   - Код:
     ```python
     report_data['factcheck'] = perform_factcheck(data)
     report_data['digital_verification'] = verify_digital_presence(data)
     ```
   - Ожидаемый результат: Поля фактчека и верификации digital будут заполнены.

4. **Совет директоров (B1)**
   - Файл: `app/pipeline/step6_board.py`
   - Задача: Изменить вызов `call_board_llm_parallel()` на последовательный.
   - Код:
     ```python
     for expert in experts:
         call_expert(expert)
         time.sleep(2)  # задержка для TPM лимита
     ```
   - Ожидаемый результат: Секция "Совет директоров" будет заполнена.

### Спринт 2: Исправление HR секции и KPI

1. **HR секция**
   - Файл: `app/templates/m4_hr_market.html`
   - Задача: Изменить шаблон для использования `key_positions`, `employees_count`, `avg_salary_market`.
   - Код:
     ```html
     <div>{{ hr_data.key_positions }}</div>
     <div>{{ hr_data.employees_count }}</div>
     <div>{{ hr_data.avg_salary_market }}</div>
     ```
   - Ожидаемый результат: HR секция будет корректно отображать данные.

2. **KPI "Нет данных"**
   - Файл: `app/templates/kpi_section.html`
   - Задача: Изменить отображение `null` значений.
   - Код:
     ```html
     <div>{{ kpi.value if kpi.value is not None else '--' }}</div>
     ```
   - Ожидаемый результат: KPI будет показывать "--" вместо "Нет данных".

### Спринт 3: Подключение HH.ru API и улучшение скрапинга

1. **HH.ru API**
   - Файл: `app/pipeline/step4_competitors.py`
   - Задача: Вызвать `hh_api.py` для получения данных.
   - Код:
     ```python
     from sources.hh_api import fetch_vacancies
     vacancies = fetch_vacancies(company_name)
     report_data['vacancies'] = vacancies
     ```
   - Ожидаемый результат: Данные о вакансиях будут добавлены в отчёт.

2. **Улучшение скрапинга**
   - Файл: `app/pipeline/step1_scrape.py`
   - Задача: Установить и использовать Scrapling.
   - Код:
     ```bash
     pip install scrapling
     ```
     ```python
     from scrapling import StealthyFetcher
     fetcher = StealthyFetcher()
     content = fetcher.fetch(company_url)
     ```
   - Ожидаемый результат: Улучшение качества скрапинга, меньше пустых данных.

### Спринт 4: ФНС и Борд

1. **ФНС данные**
   - Файл: `app/pipeline/step3_fns.py`
   - Задача: Добавить fallback на Rusprofile/SBIS.
   - Код:
     ```python
     if not fns_data:
         fns_data = fetch_rusprofile_data(company_name) or fetch_sbis_data(company_name)
     ```
   - Ожидаемый результат: Более полные данные для компаний с иностранной юрисдикцией.

2. **Борд**
   - Файл: `app/pipeline/step6_board.py`
   - Задача: Повысить TPM tier или оптимизировать вызовы.
   - Код:
     ```python
     # Обратиться к OpenAI для повышения TPM tier
     ```
   - Ожидаемый результат: Стабильная работа борда без превышения лимитов.

Эти действия должны значительно улучшить качество отчётов и устранить основные проблемы, подняв оценку продукта до 10/10.