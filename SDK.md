# Add a new SDK

We use [OpenAPI generator](https://openapi-generator.tech/) to generate ledger client SDK.`
This doc refers to the tools named [task](https://taskfile.dev/)

If you want to add a new languages, you have to configure some things.

## Configuration

First, you need to create a configuration file.

The config file must be added inside the directory sdk/configs/.
The filename MUST match the name of the involved api generator (ie java.yaml for java or php.yaml for php).

By convention, the newly added config file MUST add the following configuration : 
```
templateDir: templates/<generator name> # related to sdk/
```

Again, we use the generator name to name this directory. We also have to create the directory sdk/templates/<generator name>.

If the generator provide required properties, you MUST define the licence to MIT.

The rest of the configuration is specific to the language and should be carefully specified. 
You have to keep in mind the SDK will be hosted on github.com/numary/numary-sdk-<generator name> and so, you could have to define some properties for a language which would not exists on another language.

## Customization

Customization of the generated SDK can be achieved modifying the openapi generator builtins templates.
You can extract them for your generator using the following command (while being inside directory sdk/) :
```
task template -- <generator name>
```

The command will extract all templates inside the directory sdk/templates/<generator name>.
You can modify them as well as you want.
Please keep only modified templates.

Please read [documentation](https://openapi-generator.tech/docs/templating) for additional information.

Finally, you can check the generation using the command (while being inside sdk/):
```
task genereate -- <generator name>
```

## Publish

Once your sdk is properly generated, you have to create a PR using the template "pull_request_template". 

Using this template, you have to define how to deploy the generated SDK to the official channels. After that, your PR will be reviewed. 
If accepted, a repository will be created under github.com/numary/numary-sdk-<generator name> and the CI will be added for you on the repository.

