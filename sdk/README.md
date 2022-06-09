# Usage

Project use git submodule to fetch sdks inside 'sdks' directory.
To init them, you can clone the repository using following command : 
```
git submodule update --init --recursive
```

To update an already cloned repository, you can use : 
```
git pull --recurse-submodules
```

## taskfile

This project use taskfile. See https://taskfile.dev/#/

## Install openapi generator

```task openapi-generator:install```

## Generate code

```task generate -- go```

List of generators here : https://openapi-generator.tech/docs/generators

## Test code

```task test -- go```

Each SDK must defined it own tests and add a Taskfile.yml with a "test" task at its root.

## Push code

```task push -- go```

## Extract template

```task template -- go```

Templates will be extracted inside templates/<generator>.
