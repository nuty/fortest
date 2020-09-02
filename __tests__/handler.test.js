//  __tests__ / handler.test.js

const handler = require('../index');


test('csv format correct?', () => {
    expect(handler.checkCsvFileType("test.csv")).toBe(true);
    expect(handler.checkCsvFileType("test.png")).toBe(false);
});


test('csv content correct?', () => {
    expect(handler.checkCsvFormat({
        latitude: "-43.58299805",
        longitude: "146.89373497",
        address: "146.89373497"
    })).toBe(true);
    expect(handler.checkCsvFormat({
        latitude: "-43.58299805",
        longitude: "146.89373497",
    })).toBe(false);
    expect(handler.checkCsvFormat({
        foo: "-43.58299805",
        bar: "146.89373497",
        x: "146.89373497"
    })).toBe(false);
});
