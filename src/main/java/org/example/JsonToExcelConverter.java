package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class JsonToExcelConverter {

    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.println("❌ Путь к папке не указан.");
            return;
        }

        String folderPath = args[0].trim();

        // --- Отладочный вывод ---
        System.out.println("Введённый путь: " + folderPath);
        System.out.println("Длина пути: " + folderPath.length());

        try {
            Path path = Paths.get(folderPath).normalize();

            if (!Files.exists(path)) {
                System.err.println("❌ Указанная папка не существует.");
                return;
            }

            if (!Files.isDirectory(path)) {
                System.err.println("❌ Указанный путь не является директорией.");
                return;
            }

            File folder = path.toFile();

            // Создаём папку CleanJson внутри исходной папки
            Path cleanJsonFolderPath = Paths.get(folder.getAbsolutePath(), "CleanJson").normalize();
            if (!Files.exists(cleanJsonFolderPath)) {
                try {
                    Files.createDirectories(cleanJsonFolderPath);
                } catch (IOException e) {
                    System.err.println("❌ Не удалось создать папку CleanJson: " + e.getMessage());
                    return;
                }
            }
            File cleanJsonFolder = cleanJsonFolderPath.toFile();

            ObjectMapper mapper = new ObjectMapper();
            File[] jsonFiles = folder.listFiles((dir, name) -> name.toLowerCase().endsWith(".json"));

            if (jsonFiles == null || jsonFiles.length == 0) {
                System.out.println("❌ В папке нет JSON-файлов.");
                return;
            }

            int totalFiles = jsonFiles.length;
            System.out.println("Найдено файлов: " + totalFiles);

            // --- Фаза 1: Предварительная проверка и подготовка данных ---
            Map<String, String> dataArrayNames = new HashMap<>();
            Set<String> filesWithRootAsData = new HashSet<>();

            for (File jsonFile : jsonFiles) {
                String fileName = jsonFile.getName();
                try {
                    JsonNode rootNode = mapper.readTree(jsonFile);
                    JsonNode dataArray = rootNode.get("data");

                    if (dataArray == null || !dataArray.isArray()) {
                        System.err.println("❌ В файле " + fileName + " отсутствует массив 'data'.");
                        System.out.println("ℹ️ По умолчанию ожидалось, что данные будут в поле 'data', но оно отсутствует.");
                        System.out.print("Файл " + fileName + " уже очищен? (y/n): ");
                        Scanner scanner = new Scanner(System.in, StandardCharsets.UTF_8.name());
                        String isCleaned = scanner.nextLine().trim().toLowerCase();

                        if (isCleaned.equals("y")) {
                            filesWithRootAsData.add(fileName);
                        } else {
                            System.out.print("Введите имя поля, в котором могут быть данные (например, \"items\", \"records\"): ");
                            String dataArrayName = scanner.nextLine();
                            dataArrayNames.put(fileName, dataArrayName);
                        }
                    }
                } catch (Exception e) {
                    System.err.println("❌ Ошибка при чтении файла " + fileName + ": " + e.getMessage());
                }
            }

            // --- Фаза 2: Многопоточный парсинг ---
            ExecutorService executor = Executors.newFixedThreadPool(4); // можно изменить количество потоков
            List<Future<Void>> futures = new ArrayList<>();

            for (int fileIndex = 0; fileIndex < totalFiles; fileIndex++) {
                final File jsonFile = jsonFiles[fileIndex];
                final String fileName = jsonFile.getName();

                futures.add(executor.submit(() -> {
                    try {
                        // --- Этап 1: Чтение и очистка JSON ---
                        updateProgress("Файл: " + fileName + " | Очистка JSON", 0);
                        JsonNode rootNode = mapper.readTree(jsonFile);

                        String dataArrayName = "data"; // по умолчанию
                        JsonNode dataArray = rootNode.get(dataArrayName);

                        if (dataArray == null || !dataArray.isArray()) {
                            if (filesWithRootAsData.contains(fileName)) {
                                // Файл уже считается очищенным, корень — это массив
                                dataArray = rootNode;
                            } else {
                                // Используем имя альтернативного поля
                                dataArrayName = dataArrayNames.get(fileName);
                                dataArray = rootNode.get(dataArrayName);

                                if (dataArray == null || !dataArray.isArray()) {
                                    System.err.println("❌ Не удалось найти массив данных под названием \"" + dataArrayName + "\".");
                                    return null;
                                }
                            }
                        }

                        // Сохраняем очищенный JSON в папку CleanJson
                        String cleanedFilePath = new File(cleanJsonFolder, fileName).getAbsolutePath();
                        mapper.writerWithDefaultPrettyPrinter().writeValue(new File(cleanedFilePath), dataArray);
                        updateProgress("Файл: " + fileName + " | Очистка JSON", 100);

                        // --- Этап 2: Парсинг JSON ---
                        updateProgress("Файл: " + fileName + " | Парсинг JSON", 0);
                        List<Map<String, Object>> flatData = parseJsonToJsonNodes(new File(cleanedFilePath));
                        updateProgress("Файл: " + fileName + " | Парсинг JSON", 100);

                        // --- Этап 3: Подготовка данных ---
                        updateProgress("Файл: " + fileName + " | Подготовка данных", 0);
                        Set<String> headers = collectHeaders(flatData);
                        List<String> sortedHeaders = sortHeadersByStructure(headers);
                        updateProgress("Файл: " + fileName + " | Подготовка данных", 100);

                        // --- Этап 4: Запись в Excel ---
                        updateProgress("Файл: " + fileName + " | Формирование Excel", 0);
                        String excelFilePath = jsonFile.getAbsolutePath().replace(".json", ".xlsx");

                        int totalRows = flatData.size();

                        try (Workbook workbook = new SXSSFWorkbook(100)) {
                            Sheet sheet = workbook.createSheet("JSON Data");

                            Row headerRow = sheet.createRow(0);
                            for (int i = 0; i < sortedHeaders.size(); i++) {
                                Cell cell = headerRow.createCell(i);
                                cell.setCellValue(sortedHeaders.get(i));
                            }

                            for (int rowNum = 1; rowNum <= totalRows; rowNum++) {
                                Map<String, Object> row = flatData.get(rowNum - 1);
                                Row dataRow = sheet.createRow(rowNum);
                                for (int i = 0; i < sortedHeaders.size(); i++) {
                                    String key = sortedHeaders.get(i);
                                    Object value = row.get(key);
                                    Cell cell = dataRow.createCell(i);
                                    if (value != null) {
                                        cell.setCellValue(value.toString());
                                    }
                                }

                                int percent = (int) ((double) rowNum / totalRows * 100);
                                updateProgress("Файл: " + fileName + " | Формирование Excel", percent);
                            }

                            try (FileOutputStream fos = new FileOutputStream(excelFilePath)) {
                                workbook.write(fos);
                            }
                        }

                        updateProgress("Файл: " + fileName + " | Формирование Excel", 100);
                        System.out.println(); // Переход на новую строку после завершения этапа
                        System.out.println("✅ Данные из файла " + fileName + " успешно сохранены.");

                    } catch (Exception e) {
                        System.err.println(); // Переход на новую строку
                        System.err.println("❌ Ошибка при обработке файла " + fileName + ": " + e.getMessage());
                    }
                    return null;
                }));
            }

            // Ждём завершения всех задач
            for (Future<Void> future : futures) {
                try {
                    future.get(); // ожидаем результат
                } catch (InterruptedException | ExecutionException e) {
                    System.err.println("❌ Ошибка при выполнении задачи: " + e.getMessage());
                }
            }

            // Закрываем пул потоков
            executor.shutdown();

            System.out.println("✅ Все файлы обработаны.");
        } catch (Exception e) {
            System.err.println("❌ Ошибка при работе с папкой: " + e.getMessage());
        }
    }


    private static void updateProgress(String message, int percent) {
        final int barWidth = 50;
        double progress = percent / 100.0;
        int pos = (int) (barWidth * progress);

        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < barWidth; ++i) {
            if (i < pos) sb.append("=");
            else if (i == pos) sb.append(">");
            else sb.append(" ");
        }
        sb.append("] ").append(percent).append("% ").append(message).append("\r");

        try {
            PrintStream out = new PrintStream(System.out, true, StandardCharsets.UTF_8.name());
            out.print(sb.toString());
        } catch (UnsupportedEncodingException e) {
            System.err.println("❌ Ошибка кодировки при выводе прогресса: " + e.getMessage());
        }
    }

    private static List<Map<String, Object>> parseJsonToJsonNodes(File file) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        try (InputStream inputStream = new FileInputStream(file)) {
            JsonNode rootNode = mapper.readTree(
                    new InputStreamReader(inputStream, StandardCharsets.UTF_8)
            );

            List<Map<String, Object>> result = new ArrayList<>();

            if (rootNode.isArray()) {
                for (JsonNode node : rootNode) {
                    // Для каждого элемента создаем новую строку и обрабатываем только его
                    Map<String, Object> flatRow = new LinkedHashMap<>();
                    flattenParentToJson(node, "", flatRow);
                    result.add(flatRow);
                }
            } else {
                // Если не массив — обрабатываем один объект
                Map<String, Object> flatRow = new LinkedHashMap<>();
                flattenParentToJson(rootNode, "", flatRow);
                result.add(flatRow);
            }

            return result;
        }
    }

    private static void flattenParentToJson(JsonNode node, String parentPath, Map<String, Object> flatRow) {
        if (node.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                String newPath = parentPath.isEmpty() ? field.getKey() : parentPath + "." + field.getKey();

                if (field.getValue().isArray()) {
                    ArrayNode array = (ArrayNode) field.getValue();
                    for (int i = 0; i < array.size(); i++) {
                        JsonNode item = array.get(i);

                        if (item.isObject()) {
                            // Для каждого элемента массива рекурсивно обрабатываем его поля
                            for (Iterator<Map.Entry<String, JsonNode>> childFields = item.fields(); childFields.hasNext(); ) {
                                Map.Entry<String, JsonNode> childField = childFields.next();
                                String childKey = newPath + "[" + i + "]." + childField.getKey();
                                flatRow.put(childKey, childField.getValue().asText());
                            }

                            // Если есть вложенные массивы (например, Materials), вызываем flattenParentToJson
                            flattenParentToJson(item, newPath + "[" + i + "]", flatRow);
                        } else {
                            // Простой элемент массива
                            String childKey = newPath + "[" + i + "]";
                            flatRow.put(childKey, item.asText());
                        }
                    }
                } else if (field.getValue().isObject()) {
                    // Рекурсивно обрабатываем вложенные объекты
                    flattenParentToJson(field.getValue(), newPath, flatRow);
                } else {
                    // Простое поле
                    flatRow.put(newPath, field.getValue().asText());
                }
            }
        }
    }

    private static List<Map<String, Object>> flattenJsonWithDuplicates(JsonNode node, String parentPath, Map<String, Object> currentData) {
        List<Map<String, Object>> result = new ArrayList<>();

        if (node.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                String newPath = parentPath.isEmpty() ? field.getKey() : parentPath + "." + field.getKey();

                if (field.getValue().isArray()) {
                    for (int i = 0; i < field.getValue().size(); i++) {
                        JsonNode arrayItem = field.getValue().get(i);
                        String arrayKey = newPath + "[" + i + "]";

                        Map<String, Object> newData = new LinkedHashMap<>(currentData);
                        List<Map<String, Object>> nestedResult = flattenJsonWithDuplicates(arrayItem, arrayKey, newData);
                        result.addAll(nestedResult);
                    }
                } else if (field.getValue().isObject()) {
                    Map<String, Object> newData = new LinkedHashMap<>(currentData);
                    newData.put(newPath, field.getValue().toString()); // Сохраняем значение как JSON-строку
                    result.addAll(flattenJsonWithDuplicates(field.getValue(), newPath, newData));
                } else {
                    currentData.put(newPath, field.getValue().asText());
                }
            }
        }

        result.add(currentData);
        return result;
    }

    private static Set<String> collectHeaders(List<Map<String, Object>> data) {
        Set<String> headers = new HashSet<>();
        for (Map<String, Object> row : data) {
            headers.addAll(row.keySet());
        }
        return headers;
    }

    private static List<String> sortHeadersByStructure(Set<String> headers) {
        List<String> sortedHeaders = new ArrayList<>(headers);
        sortedHeaders.sort((a, b) -> {
            if (a == null || b == null || a.isEmpty() || b.isEmpty()) {
                return a == null ? -1 : b == null ? 1 : a.compareTo(b);
            }

            // Разбиваем строки на части
            String[] aParts = splitPath(a);
            String[] bParts = splitPath(b);

            for (int i = 0; i < Math.min(aParts.length, bParts.length); i++) {
                String aPart = aParts[i];
                String bPart = bParts[i];

                int indexA = extractArrayIndexFromPart(aPart);
                int indexB = extractArrayIndexFromPart(bPart);

                // Если обе части содержат индексы — сравниваем их как числа
                if (indexA != -1 && indexB != -1) {
                    int cmp = Integer.compare(indexA, indexB);
                    if (cmp != 0) return cmp;
                } else {
                    // Иначе — обычное сравнение по строке
                    int cmp = aPart.compareTo(bPart);
                    if (cmp != 0) return cmp;
                }
            }

            // Если одна строка длиннее другой — та, которая короче, идёт первой
            return Integer.compare(aParts.length, bParts.length);
        });

        return sortedHeaders;
    }

    private static String[] splitPath(String path) {
        List<String> parts = new ArrayList<>();
        int start = 0;

        while (start < path.length()) {
            int dotIndex = path.indexOf('.', start);
            int bracketIndex = path.indexOf('[', start);

            if (dotIndex == -1 && bracketIndex == -1) {
                parts.add(path.substring(start));
                break;
            }

            int nextIndex = Math.min(
                    dotIndex != -1 ? dotIndex : Integer.MAX_VALUE,
                    bracketIndex != -1 ? bracketIndex : Integer.MAX_VALUE
            );

            if (nextIndex == Integer.MAX_VALUE) break;

            if (path.charAt(nextIndex) == '.') {
                parts.add(path.substring(start, nextIndex));
                start = nextIndex + 1;
            } else if (path.charAt(nextIndex) == '[') {
                int closeBracket = path.indexOf(']', nextIndex);
                if (closeBracket != -1) {
                    parts.add(path.substring(start, closeBracket + 1));
                    start = closeBracket + 1;
                } else {
                    parts.add(path.substring(start));
                    break;
                }
            }
        }

        return parts.toArray(new String[0]);
    }

    private static int extractArrayIndexFromPart(String part) {
        int openBracket = part.indexOf('[');
        if (openBracket == -1) return -1;
        int closeBracket = part.indexOf(']');
        if (closeBracket == -1) return -1;

        try {
            return Integer.parseInt(part.substring(openBracket + 1, closeBracket));
        } catch (NumberFormatException | StringIndexOutOfBoundsException e) {
            return -1;
        }
    }
}
