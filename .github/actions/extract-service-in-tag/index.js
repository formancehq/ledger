const value = process.argv[2];

const getValue = (string) => {
    const value = string.match(/(?<=\/)[^\/]+(?=\/)/)[0];
    return value;
};

console.log(getValue(value));
