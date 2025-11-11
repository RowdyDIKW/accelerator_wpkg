"""
CIA (Consumable Information Area) Module
Dynamically imports all functions from Python files in this directory.
"""

import importlib
import inspect
import ast
import functools
from pathlib import Path

# Get the current directory path
_current_dir = Path(__file__).parent

# List to keep track of all imported function names for __all__
__all__ = []


def _import_modules():
    """Dynamically import all Python modules in the current directory and
    subpackages. Skip files and directories that start with an underscore.
    """
    global __all__

    python_files = [
        p
        for p in _current_dir.rglob("*.py")
        if p.is_file() and p.name != "__init__.py"
        and not any(part.startswith("_") for part in p.relative_to(_current_dir).parts)
    ]

    for p in python_files:
        module_rel = p.relative_to(_current_dir).with_suffix("").as_posix().replace("/", ".")
        try:
            # Parse the file to discover top-level function names without executing it
            src = p.read_text(encoding="utf-8")
            parsed = ast.parse(src)
            func_names = [n.name for n in parsed.body if isinstance(n, ast.FunctionDef)]

            for fname in func_names:
                if fname in globals():
                    # don't override existing names
                    continue

                # create a lazy proxy that imports module and calls the real function
                def make_proxy(mod_name, fn):
                    @functools.wraps(fn)
                    def proxy(*args, **kwargs):
                        module = importlib.import_module(f".{mod_name}", package=__name__)
                        real = getattr(module, fn.__name__ if hasattr(fn, '__name__') else fn)
                        return real(*args, **kwargs)

                    return proxy

                # create a dummy callable with the target name for wrapping purposes
                dummy = type(fname, (), {})
                proxy_fn = make_proxy(module_rel, dummy)
                proxy_fn.__name__ = fname
                globals()[fname] = proxy_fn
                if fname not in __all__:
                    __all__.append(fname)

        except Exception as e:
            # If file can't be read or parsed, warn and continue
            print(f"Warning: Could not process '{module_rel}': {e}")


# Perform the dynamic import
_import_modules()

# Sort __all__ for consistency
__all__.sort()