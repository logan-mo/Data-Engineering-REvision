"""
Here is a brief workflow from my notes that I think should work for generating documentation:

1. All tables that are joined to each other, group them. Let's call these "connective clusters" from now on.
2. Grab all Primary and Foreign keys and describe the connections between tables
within a connective cluster in one paragraph per cluster
3. Island tables, tables that have no foreign key relationship with any other table, generate documentation for them first.
Documentation here means explain evey column and its type in one line to the best of the models understanding
as well as one explanation for the whole table's purpose.
4. For any fields whose purpose is not clear, generate question that the user can respond to,
which will describe the column or table more accurately
5. If a certain column name is sematically worthless, rename it. Store the renaming.
6. When the user describes a column, check whether a new name for the column will make its intent more clear. 
Store the renaming
7. Next we document connective cluster. If we have a tree-cluster, where there is one node that has no 
Foreign keys and all tables have foreign keys referring to it, then we start from said root table. Conversely
if no table is a root and all of them have circular dependencies, we start with the tables with the
smaller number of Foreign keys, work our way up the table graph until all tables in a cluster have been documented.
The documentation for each table would be exactly the same as the table documentation in point 3.
Lastly we also generate a description for the whole cluster and give it a cluster name. We store this.
8. After all documentation has been generated for the defined scope from the data source, user has been prompted
to describe columns, and all columns have been renamed to be semantically significant we move on.
9. Now we describe Databases / Schemas
10. The we cluster similar database / Schema together, explain clusters, and give the clusters a name
11. At this point, we should have enough hierarchical documentation that if a user asks a question about the data,
we should be able to navigate the hierarchy to the exact DB, connective cluster, table or even column
to answer the users questions. This will be the "Understanding the data" part of the ML Pipeline.
"""

import os
import pandas as pd
from langgraph.types import Send
from pydantic import BaseModel, Field
from typing import List, Dict, Annotated
from IPython.display import display, Markdown
from typing_extensions import Literal, TypedDict
from langchain.chat_models import init_chat_model
from langgraph.graph import StateGraph, START, END
from langgraph.checkpoint.memory import InMemorySaver
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage, SystemMessage
