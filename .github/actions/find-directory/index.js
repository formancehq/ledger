const fs = require('fs');
const path = require('path');

function findDockerFile(dir) {
    let results = [];
    const list = fs.readdirSync(dir);
    list.forEach(file => {
        file = path.resolve(dir, file);
        const stat = fs.statSync(file);
        if (stat && stat.isDirectory()) {
            /* if it is a directory, recurse */
            results = results.concat(findDockerFile(file));
        } else {
            if (path.basename(file) === "Dockerfile") {
                results.push(path.basename(path.dirname(file)));
            }
        }
    });
    return results;
}
console.log(JSON.stringify(findDockerFile("./components"),null,0));
