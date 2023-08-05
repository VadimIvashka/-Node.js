const fs = require('fs');
const readline = require('readline');

// Функция для сортировки и записи данных во временный файл
async function sortAndWriteToFile(data, outputPath) {
    const sortedData = data.sort();
    return new Promise((resolve, reject) => {
        const writeStream = fs.createWriteStream(outputPath, { flags: 'a' });
        writeStream.on('finish', () => resolve());
        writeStream.on('error', (err) => reject(err));
        sortedData.forEach((line) => writeStream.write(`${line}\n`));
        writeStream.end();
    });
}

// Функция для слияния двух файлов и записи результата в третий файл
async function mergeFiles(file1Path, file2Path, outputPath) {
    return new Promise((resolve, reject) => {
        const readStream1 = fs.createReadStream(file1Path);
        const readStream2 = fs.createReadStream(file2Path);
        const writeStream = fs.createWriteStream(outputPath);

        const rl1 = readline.createInterface({ input: readStream1 });
        const rl2 = readline.createInterface({ input: readStream2 });

        let line1 = '';
        let line2 = '';

        function getNextLine(rl, prevLine) {
            return new Promise((resolve) => {
                rl.once('line', (line) => resolve(line || prevLine));
            });
        }

        async function merge() {
            while (line1 || line2) {
                if (!line1) {
                    line1 = await getNextLine(rl1, line1);
                }
                if (!line2) {
                    line2 = await getNextLine(rl2, line2);
                }

                if (line1 && line2) {
                    if (line1 < line2) {
                        writeStream.write(`${line1}\n`);
                        line1 = '';
                    } else {
                        writeStream.write(`${line2}\n`);
                        line2 = '';
                    }
                } else if (line1) {
                    writeStream.write(`${line1}\n`);
                    line1 = '';
                } else if (line2) {
                    writeStream.write(`${line2}\n`);
                    line2 = '';
                }
            }

            writeStream.end();
            await Promise.all([
                new Promise((resolve) => rl1.close(resolve)),
                new Promise((resolve) => rl2.close(resolve)),
            ]);
            resolve();
        }

        merge();
    });
}

async function externalSort(inputPath, outputPath) {
    try {
        const readStream = fs.createReadStream(inputPath);
        const rl = readline.createInterface({ input: readStream });

        const chunkSize = 500 * 1024 * 1024; // 500 МБ
        let tempFileIndex = 0;
        let tempData = [];

        for await (const line of rl) {
            tempData.push(line);
            if (Buffer.byteLength(tempData.join('\n'), 'utf8') >= chunkSize) {
                tempData.sort();
                const tempFilePath = `temp${tempFileIndex}.txt`;
                await sortAndWriteToFile(tempData, tempFilePath);
                tempData = [];
                tempFileIndex++;
            }
        }

        if (tempData.length > 0) {
            tempData.sort();
            const tempFilePath = `temp${tempFileIndex}.txt`;
            await sortAndWriteToFile(tempData, tempFilePath);
            tempFileIndex++;
        }

        rl.close();

        if (tempFileIndex === 1) {
            // Если только один временный файл, это и есть отсортированный результат
            fs.renameSync(`temp0.txt`, outputPath);
            return;
        }

        // Последовательное слияние всех временных файлов
        let currentMergeIndex = 0;
        while (currentMergeIndex < tempFileIndex - 1) {
            const mergedFilePath = `temp_merged${currentMergeIndex}.txt`;
            await mergeFiles(
                `temp${currentMergeIndex}.txt`,
                `temp${currentMergeIndex + 1}.txt`,
                mergedFilePath
            );
            currentMergeIndex++;
        }

        // Переименовываем последний временный файл в выходной файл
        fs.renameSync(`temp_merged${currentMergeIndex - 1}.txt`, outputPath);

        // Удаляем все временные файлы
        for (let i = 0; i < tempFileIndex; i++) {
            fs.unlinkSync(`temp${i}.txt`);
            if (i < tempFileIndex - 1) {
                fs.unlinkSync(`temp_merged${i}.txt`);
            }
        }

        console.log('Сортировка успешно завершена!');
    } catch (err) {
        console.error('Произошла ошибка:', err);
    }
}

const inputFile = 'input.txt'; // путь к исходному файлу размером 1 ТБ
const outputFile = 'output.txt'; // путь к выходному отсортированному файлу

externalSort(inputFile, outputFile);