# Mini-RxJava — Отчёт

## Структура проекта

```
src/
├── main/java/
│   ├── rx/
│   │   ├── core/
│   │   │   ├── Observer.java          — интерфейс с onNext / onError / onComplete
│   │   │   ├── Emitter.java           — handle для пользовательского кода внутри create()
│   │   │   ├── SafeObserver.java      — обёртка, обеспечивающая контракт Rx
│   │   │   └── Observable.java        — холодный поток: create, just, map, filter, flatMap,
│   │   │                                subscribeOn, observeOn, subscribe
│   │   ├── disposable/
│   │   │   ├── Disposable.java        — интерфейс отмены подписки
│   │   │   └── AtomicDisposable.java  — потокобезопасная реализация на AtomicBoolean
│   │   └── schedulers/
│   │       ├── Scheduler.java         — интерфейс: execute(Runnable)
│   │       └── Schedulers.java        — три реализации + фабрика
│   └── demo/
│       └── Demo.java                  — демонстрационная программа (8 сценариев)
└── test/java/
    └── rx/
        └── RxTests.java               — 24 юнит-теста без внешних зависимостей
run.sh                                 — компиляция и запуск
```

---

## Архитектура системы

### Паттерн «Наблюдатель» (Observer pattern)

Ядро библиотеки — это пара `Observable<T>` / `Observer<T>`:

```
Observable<T>  ──subscribe(Observer)──►  Observer<T>
     │                                        ▲
     │  хранит OnSubscribe<T>                 │
     │  (лямбда-источник)             SafeObserver<T>
     │                                (контракт Rx)
     └─────────────────────────────────────────┘
```

`Observable` — **холодный** поток: источник запускается заново для каждого нового подписчика. Это упрощает рассуждения о побочных эффектах.

### SafeObserver и контракт Rx

Rx Grammar определяет, что поток событий должен соответствовать выражению:
```
onNext*  (onError | onComplete)?
```
Т.е. ноль или более `onNext`, затем не более одного терминального события.

`SafeObserver<T>` является единственным местом, где этот контракт проверяется:
- Использует `AtomicBoolean terminated` для предотвращения двойного терминального события.
- Использует `AtomicDisposable` для подавления событий после `dispose()`.
- Перехватывает исключения из `downstream.onNext()` и перенаправляет их в `onError()`.

Операторы (`map`, `filter`, `flatMap`) не проверяют контракт самостоятельно — они пишут «простой» код и делегируют SafeObserver.

### Оператор цепочки (Operator chaining)

Каждый оператор создаёт **новый** `Observable`, который «оборачивает» предыдущий:

```
just(1,2,3)          ← источник
  .filter(x%2==0)    ← Observable A (подписывается на источник)
  .map(x -> x*x)     ← Observable B (подписывается на A)
  .subscribe(...)    ← запускает сборку цепочки снизу вверх
```

Цепочка строится лениво — ничего не выполняется до вызова `subscribe()`.

### Emitter vs Observer

`Emitter<T>` — чистый API для пользовательского кода внутри `Observable.create()`.  
`Observer<T>` — контракт для потребителей потока.  
Оба выглядят похоже, но разделены намеренно: пользователь пишет через `Emitter`, а `SafeObserver` является посредником между `Emitter` и нижестоящим `Observer`.

---

## Schedulers: принципы работы и различия

### Интерфейс

```java
public interface Scheduler {
    void execute(Runnable task);
    default void shutdown() {}
}
```

Простота — намеренная: `Scheduler` не знает о `Observable`, он только выполняет задачи.

### Три реализации

| Scheduler | Пул потоков | Назначение | Риски |
|---|---|---|---|
| `IOThreadScheduler` | `Executors.newCachedThreadPool()` | Ввод-вывод: сеть, диск, БД | При шторме запросов может создать тысячи потоков |
| `ComputationScheduler` | `Executors.newFixedThreadPool(nCPU)` | CPU-bound: вычисления в памяти | Блокирующие задачи «голодят» ядра |
| `SingleThreadScheduler` | `Executors.newSingleThreadExecutor()` | Последовательная обработка, состояние | Узкое горлышко при высокой нагрузке |

### subscribeOn — где работает источник

```java
observable
  .subscribeOn(Schedulers.io())  // источник запускается в IO-потоке
  .subscribe(observer);
```

`subscribeOn` оборачивает вызов `source.subscribe(downstream)` в `scheduler.execute(...)`. Имеет эффект только **один раз** на цепочку — самый близкий к источнику `subscribeOn` «побеждает».

### observeOn — где работает наблюдатель

```java
observable
  .observeOn(Schedulers.single())  // каждый onNext/onError/onComplete → в single-поток
  .subscribe(observer);
```

`observeOn` оборачивает каждый вызов `downstream.onNext/onError/onComplete` в `scheduler.execute(...)`. В отличие от `subscribeOn`, **каждый** `observeOn` в цепочке вводит границу смены потока.

### Типичный паттерн

```
IO-поток:         [source]──onNext──►[map]
                                         │
                                    observeOn
                                         │
Single-поток:                        [observer]
```

---

## Операторы

### map(Function)
Применяет функцию к каждому элементу. Исключение из функции → `onError`.

### filter(Predicate)
Пропускает только элементы, для которых предикат возвращает `true`. Исключение → `onError`.

### flatMap(Function → Observable)
Для каждого элемента создаёт внутренний `Observable`, подписывается на него синхронно и передаёт все его элементы дальше. Реализует **concat-семантику** (последовательная подписка) для простоты и детерминированного порядка.

---

## Обработка ошибок

1. **Исключение в источнике** (`create`-лямбда): перехватывается в `Observable.subscribe()` и направляется в `safeObserver.onError()`.
2. **Исключение в операторе** (`map`, `filter`): перехватывается внутри оператора и направляется в `downstream.onError()`.
3. **Терминальность**: после первого `onError` дальнейшие события не доставляются (`SafeObserver`).
4. **Исключение в `onError` наблюдателя**: передаётся в `Thread.UncaughtExceptionHandler` (как в RxJava 2+), чтобы не потерять информацию об ошибке.

---

## Disposable и отмена подписки

```java
Disposable d = observable.subscribe(observer);
d.dispose(); // прекращает доставку событий
```

`AtomicDisposable` — потокобезопасная реализация на `AtomicBoolean`. `SafeObserver` проверяет `isDisposed()` перед каждым `onNext`.

**Важный нюанс с синхронными источниками**: если источник `Observable.create(...)` не использует `subscribeOn`, он выполняется целиком внутри вызова `subscribe()`, до того как этот вызов вернёт `Disposable`. Поэтому отмену из `onNext`-обработчика нужно реализовывать через внешний `AtomicBoolean`-флаг, который источник сам проверяет. При асинхронных источниках (с `subscribeOn`) `Disposable` доступен до начала доставки элементов.

---

## Тестирование

### Запуск

```bash
bash run.sh
```

Тесты написаны без внешних зависимостей (JUnit не используется): каждый тест — статический метод, регистрируемый вручную в `main`. Провальный тест не останавливает прогон.

### Покрытые сценарии (24 теста)

**Блок 1 — Базовые компоненты**
- `just` эмитирует элементы в порядке
- `create` эмитирует и завершается
- `empty` вызывает только `onComplete`
- `error` вызывает только `onError`
- `fromIterable` обходит коллекцию

**Блок 2 — Операторы**
- `map` преобразует каждый элемент
- `map` направляет исключение в `onError`
- `filter` пропускает подходящие элементы
- `filter` удаляет неподходящие
- `flatMap` разворачивает каждый элемент
- `flatMap` с ошибкой во внутреннем Observable
- Цепочка `filter → map → flatMap`

**Блок 3 — Disposable**
- Синхронный источник + флаг отмены
- Асинхронный источник + `dispose()` из `onNext`
- `SafeObserver`: нет событий после `onComplete`
- `SafeObserver`: нет событий после `onError`

**Блок 4 — Обработка ошибок**
- Исключение в источнике → `onError`
- Исключение в `map` → `onError`
- Исключение в `filter` → `onError`

**Блок 5 — Schedulers**
- `subscribeOn`: источник работает в другом потоке
- `observeOn`: наблюдатель получает события в другом потоке
- `subscribeOn` + `observeOn`: каждый в своём потоке
- `ComputationScheduler`: параллелизм ≤ nCPU
- `SingleThreadScheduler`: порядок выполнения сохраняется

### Примеры использования

```java
// 1. Простая трансформация
Observable.just(1, 2, 3, 4, 5)
    .filter(x -> x % 2 == 0)
    .map(x -> "even: " + x)
    .subscribe(System.out::println);

// 2. Асинхронный I/O с переключением потоков
Observable.<String>create(emitter -> {
    emitter.onNext(readFromNetwork());
    emitter.onComplete();
})
.subscribeOn(Schedulers.io())
.map(String::toUpperCase)
.observeOn(Schedulers.single())
.subscribe(
    item  -> updateUI(item),
    error -> showError(error),
    ()    -> hideSpinner()
);

// 3. flatMap для развёртки
Observable.just("Alice", "Bob")
    .flatMap(name -> Observable.just(name + "-1", name + "-2"))
    .subscribe(System.out::println);
// Alice-1, Alice-2, Bob-1, Bob-2

// 4. Обработка ошибок
Observable.just("10", "bad", "30")
    .map(Integer::parseInt)
    .subscribe(
        item  -> System.out.println("parsed: " + item),
        error -> System.out.println("error: " + error.getMessage())
    );
// parsed: 10
// error: For input string: "bad"

// 5. CompletableFuture-стиль через submit() (из ThreadPool-задания)
CompletableFuture.supplyAsync(() -> heavyComputation(), Schedulers.computation()::execute);
```
#   R x J a v a  
 