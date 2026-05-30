# Mini-RxJava - отчет

Репозиторий содержит учебную реализацию основных идей RxJava: `Observable`, `Observer`,
операторы преобразования, планировщики потоков, обработку ошибок и отмену подписки.

## Быстрый запуск

Для Windows:

```powershell
.\run.cmd
```

Альтернативно через PowerShell:

```powershell
powershell -ExecutionPolicy Bypass -File .\run.ps1
```

Для Linux/macOS или Git Bash:

```bash
bash run.sh
```

Скрипты компилируют исходный код из `src/main/java` и тесты из `src/test/java`, затем
запускают тестовый набор `rx.RxTests` и демонстрационную программу `demo.Demo`.

## Структура проекта

```text
src/
|-- main/java/
|   |-- demo/
|   |   `-- Demo.java
|   `-- rx/
|       |-- core/
|       |   |-- Emitter.java
|       |   |-- Observable.java
|       |   |-- Observer.java
|       |   `-- SafeObserver.java
|       |-- disposable/
|       |   |-- AtomicDisposable.java
|       |   `-- Disposable.java
|       `-- schedulers/
|           |-- Scheduler.java
|           `-- Schedulers.java
`-- test/java/
    `-- rx/
        `-- RxTests.java
run.sh
run.cmd
run.ps1
```

## Архитектура системы

### Observer pattern

Ядро библиотеки - пара `Observable<T>` и `Observer<T>`.

```text
Observable<T> --subscribe(Observer)--> Observer<T>
     |                                  ^
     | хранит OnSubscribe<T>            |
     | источник событий                 |
     `-------------------------- SafeObserver<T>
```

`Observable` реализован как холодный поток: источник начинает работу только после
`subscribe()` и запускается заново для каждого подписчика. Это делает поведение
предсказуемым и упрощает тестирование.

`Observer<T>` содержит три метода:

- `onNext(T item)` - получение очередного элемента;
- `onError(Throwable t)` - получение ошибки;
- `onComplete()` - успешное завершение потока.

`Emitter<T>` используется внутри `Observable.create(...)`. Он похож на `Observer`, но
предназначен для пользовательского кода, который эмитирует события.

### SafeObserver и Rx-контракт

Поток событий должен соответствовать правилу:

```text
onNext* (onError | onComplete)?
```

То есть после `onError` или `onComplete` новые события не должны попадать подписчику.
За это отвечает `SafeObserver<T>`:

- хранит `AtomicBoolean terminated` и не допускает повторных терминальных событий;
- содержит `AtomicDisposable`, который подавляет доставку после `dispose()`;
- перехватывает исключения из `onNext` и переводит их в `onError`;
- ошибки из терминальных обработчиков отправляет в `UncaughtExceptionHandler`.

### Observable

`Observable<T>` хранит функциональный источник `OnSubscribe<T>`. Все операторы создают
новый `Observable`, который оборачивает предыдущий. Цепочка собирается лениво: до
вызова `subscribe()` ничего не выполняется.

Пример:

```java
Observable.just(1, 2, 3, 4)
    .filter(x -> x % 2 == 0)
    .map(x -> x * x)
    .subscribe(System.out::println);
```

## Реализованные операторы

### map

`map(Function<? super T, ? extends R> mapper)` преобразует каждый элемент потока.
Если функция выбрасывает исключение, оно передается в `onError`.

```java
Observable.just(1, 2, 3)
    .map(x -> "item-" + x)
    .subscribe(System.out::println);
```

### filter

`filter(Predicate<? super T> predicate)` пропускает только элементы, для которых
предикат вернул `true`. Исключение из предиката также передается в `onError`.

```java
Observable.just(1, 2, 3, 4)
    .filter(x -> x % 2 == 0)
    .subscribe(System.out::println);
```

### flatMap

`flatMap(Function<? super T, ? extends Observable<? extends R>> mapper)` превращает
каждый элемент во внутренний `Observable` и передает его элементы дальше. В этой
учебной реализации внутренние потоки обрабатываются последовательно, что дает
детерминированный порядок событий.

```java
Observable.just("Alice", "Bob")
    .flatMap(name -> Observable.just(name + "-1", name + "-2"))
    .subscribe(System.out::println);
```

## Schedulers

Планировщик описан интерфейсом:

```java
public interface Scheduler {
    void execute(Runnable task);
    default void shutdown() {}
}
```

Он не зависит от `Observable` и занимается только запуском `Runnable`.

| Scheduler | Основа | Назначение |
|---|---|---|
| `IOThreadScheduler` | `Executors.newCachedThreadPool()` | I/O-задачи: сеть, диск, база данных |
| `ComputationScheduler` | `Executors.newFixedThreadPool(nCPU)` | CPU-bound вычисления |
| `SingleThreadScheduler` | `Executors.newSingleThreadExecutor()` | последовательная обработка и работа с состоянием |

### subscribeOn

`subscribeOn(Scheduler scheduler)` переносит выполнение источника в указанный
планировщик.

```java
Observable.<Integer>create(emitter -> {
    emitter.onNext(1);
    emitter.onComplete();
})
.subscribeOn(Schedulers.io())
.subscribe(System.out::println);
```

### observeOn

`observeOn(Scheduler scheduler)` переносит вызовы `onNext`, `onError` и `onComplete`
на указанный планировщик.

```java
Observable.just(1, 2, 3)
    .observeOn(Schedulers.single())
    .subscribe(System.out::println);
```

Типичный сценарий: источник выполняется в `IOThreadScheduler`, вычисления остаются в
цепочке операторов, а финальная обработка переводится в `SingleThreadScheduler`.

## Disposable

`Disposable` позволяет отменить подписку:

```java
Disposable disposable = observable.subscribe(System.out::println);
disposable.dispose();
```

`AtomicDisposable` реализован через `AtomicBoolean`, поэтому безопасен для
многопоточных сценариев. `SafeObserver` проверяет состояние перед доставкой событий.

В синхронных источниках `subscribe()` возвращает `Disposable` уже после выполнения
источника. Поэтому для демонстрации ранней остановки синхронного источника используется
внешний флаг, а для асинхронного источника - `subscribeOn`, где `Disposable` доступен
до доставки следующих элементов.

## Обработка ошибок

Реализованы основные сценарии:

- исключение в `Observable.create(...)` перехватывается и передается в `onError`;
- исключения в `map` и `filter` передаются в `onError`;
- после первого `onError` или `onComplete` новые события не доставляются;
- ошибка из `onNext` подписчика также переводится в `onError`.

Пример:

```java
Observable.just("10", "bad", "30")
    .map(Integer::parseInt)
    .subscribe(
        item -> System.out.println("parsed: " + item),
        error -> System.out.println("error: " + error.getMessage()),
        () -> System.out.println("complete")
    );
```

## Тестирование

Тесты находятся в `src/test/java/rx/RxTests.java`. Они написаны без внешних
зависимостей, чтобы проект можно было проверить обычными `javac` и `java`.

Покрытые сценарии:

- создание потоков через `create`, `just`, `empty`, `error`, `fromIterable`;
- операторы `map`, `filter`, `flatMap` и их цепочки;
- доставка ошибок из источника и операторов;
- контракт `SafeObserver`: нет событий после `onError` и `onComplete`;
- отмена подписки через `Disposable`;
- работа `subscribeOn` и `observeOn`;
- ограничения `ComputationScheduler`;
- последовательность выполнения в `SingleThreadScheduler`.

Последний локальный прогон:

```text
Results: 24 passed, 0 failed
```

## Демонстрация

`demo.Demo` показывает:

- простую цепочку `filter + map`;
- создание собственного источника через `Observable.create`;
- разворачивание элементов через `flatMap`;
- обработку ошибок;
- отмену подписки;
- `subscribeOn` и `observeOn`;
- параллельные задачи на `ComputationScheduler`;
- комбинированный сценарий с IO-источником и single-подписчиком.
