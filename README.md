
# data-harvest-reader

## Features

1. **Reading Various File Formats**
2. **Directory and ZIP File Handling**
3. **Data Joining**
4. **Deduplication**
5. **Custom Filters**
6. **Logging**

## Installation Requirements

```bash
pip install polars loguru
```

## Usage

### Initialization

```python
from data_harvest_reader import DataReader

data_reader = DataReader(log_to_file=True, log_file="data_reader.log")
```

### Reading Data

#### From Directory

```python
data = data_reader.read_data('path/to/directory', join_similar=True)
```

#### From ZIP File

```python
data = data_reader.read_data('path/to/zipfile.zip', join_similar=False)
```

### Applying Deduplication

```python
duplicated_subset_dict = {'file1': ['column1', 'column2']}
data = data_reader.read_data('path/to/source', duplicated_subset_dict=duplicated_subset_dict)
```

### Applying Filters

```python
filter_subset = {
    'file1': [{'column': 'Col1', 'operation': '>', 'values': 100},
              {'column': 'Col2', 'operation': '==', 'values': 'Value'}]
}

data = data_reader.read_data('path/to/source', filter_subset=filter_subset)
```

### Handling Exceptions

```python
try:
    data = data_reader.read_data('path/to/source')
except UnsupportedFormatError:
    print("Unsupported file format provided")
except FilterConfigurationError:
    print("Error in filter configuration")
```

## Example

```python
data_reader = DataReader()

data = data_reader.read_data(r'C:\path\to\data', join_similar=True,
                             filter_subset={'example_file': [{'column': 'Age', 'operation': '>', 'values': 30}]})
```

## Contributing to DataReader

### Getting Started

1. **Fork the Repository**: Start by forking the main repository. This creates your own copy of the project where you can make changes.
2. **Clone the Forked Repository**: Clone your fork to your local machine. This step allows you to work on the codebase directly.
3. **Set Up the Development Environment**: Ensure you have all necessary dependencies installed. It's recommended to use a virtual environment.
4. **Create a New Branch**: Always create a new branch for your changes. This keeps the main branch stable and makes reviewing changes easier.

### Making Contributions

1. **Make Your Changes**: Implement your feature, fix a bug, or make your proposed changes. Ensure your code adheres to the project's coding standards and guidelines.
2. **Test Your Changes**: Before submitting, test your changes thoroughly. Write unit tests if applicable, and ensure all existing tests pass.
3. **Document Your Changes**: Update the documentation to reflect your changes. If you're adding a new feature, include usage examples.
Push your changes to your fork on GitHub.
4. **Commit Your Changes**: Make concise and clear commit messages, describing what each commit does.
5. **Push to Your Fork**: Push your changes to your fork on GitHub.
6. **Create a Pull Request (PR)**: Go to the original `DataReader` repository and create a pull request from your fork. Ensure you describe your changes in detail and link any relevant issues.

### Review Process
After submitting your PR, the maintainers will review your changes. Be responsive to feedback:

1. **Respond to Comments**: If the reviewers ask for changes, make them promptly. Discuss any suggestions or concerns.
2. **Update Your PR**: If needed, update your PR based on feedback. This may involve adding more tests or tweaking your approach.

### Final Steps
Once your PR is approved:

1. **Merge**: The maintainers will merge your changes into the main codebase.
2. **Stay Engaged**: Continue to stay involved in the project. Look out for feedback from users on your new feature or fix.

## Conclusion

Contributing to `DataReader` is a rewarding experience that benefits the entire user community. Your contributions help make `DataReader` a more robust and versatile tool. We welcome developers of all skill levels and appreciate every form of contribution, from code to documentation. Thank you for considering contributing to `DataReader`!