"""
This module defines a base class for derived database classes that store calculation data.
"""

import datetime
import os
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass, field, asdict

from maggma.stores import MongoStore, MongoURIStore, S3Store
from monty.json import jsanitize
from monty.serialization import loadfn
from pymongo import MongoClient, ReturnDocument
from pymongo.uri_parser import parse_uri

from atomate.utils.utils import get_logger

__author__ = "Kiran Mathew"
__credits__ = "Anubhav Jain"
__email__ = "kmathew@lbl.gov"

logger = get_logger(__name__)

@dataclass
class DbConfigFile:
    """Represents db.json configuration file
    """
    host: str = None
    port: int = None
    database: str = None
    collection: str = ""
    host_uri: str = None
    maggma_store: dict = field(default_factory=dict)
    maggma_store_prefix: str = "atomate"
    mongoclient_kwargs: dict = field(default_factory=dict)
    admin_user: str = None
    admin_password: str = None
    readonly_user: str = None
    readonly_password: str = None
    authsource: str = None

@dataclass
class DbConfig:
    """Represents CalcDb configuration
    
    Note: order of fields needs to mirror CalcDb.__init__ documentation. Should work for python 3.7+
    """
    host: str = None
    port: int = None
    database: str = None
    collection: str = None
    user: str = None
    password: str = None
    host_uri: str = None
    maggma_store_prefix: str = "atomate"
    maggma_store_kwargs: dict = field(default_factory=dict)
    authsource: str = None


def update_from_env(instance:dataclass, prefix:str):
    """
    update a dataclass instance from environment variables

    environment variables, if set, take precedence.
    """
    #assert isinstance(instance, dataclass)
    for k in instance.__dataclass_fields__:
        env_key = prefix + k.upper()
        if env_key in os.environ:
            setattr(instance, k, os.environ[env_key])
    return instance

class CalcDb(metaclass=ABCMeta):
    def __init__(
        self,
        *args,
        **kwargs,
    ):
        """
        Object to handle storing calculation data to MongoDB databases.
        The results of calculations will be parsed by a Drone and
        CalcDb is only responsible for putting that data into the database

        Args:
            host: name of MongoDB host
            port: the port number
            database: the name of the MongoDB database
            collection: the collection were the parsed dictionaries will be stored
            user: MongoDB authentication username
            password: MongoDB authentication password
            host_uri: If the uri designation of the mongodb is provided,
                        other authentication information will be ignored
            maggma_store_kwargs: additional kwargs for mongodb login.
                Currently supports:
                    S3 store kwargs:
                        "bucket" : the S3 bucket where the data is stored
                        "s3_profile" : the S3 profile that contains the login information
                                        typically found at ~/.aws/credentials
                        "compress" : Whether compression is used
                        "endpoint_url" : the url used to access the S3 store
            maggma_store_prefix: when using maggma stores, you can set the prefix string.

            **kwargs:
        """
        kwargs_unknown = {k: kwargs.pop(k) for k in list(kwargs.keys()) if k not in DbConfig.__dataclass_fields__}
        cfg = update_from_env(DbConfig(*args, **kwargs), prefix="ATOMATE_")

        self.maggma_store_prefix = cfg.maggma_store_prefix
        self._maggma_store_kwargs = cfg.maggma_store_kwargs
        self.host = cfg.host
        self.db_name = cfg.database
        self.user = cfg.user
        self.password = cfg.password
        self.port = cfg.port
        self.host_uri = cfg.host_uri

        self._maggma_store_type = None
        if "bucket" in self._maggma_store_kwargs:
            self._maggma_store_type = "s3"
        # Implement additional maggma stores here as needed

        self._maggma_stores = {}

        if cfg.host_uri is not None:
            dd_uri = parse_uri(cfg.host_uri)
            if dd_uri["database"] is not None:
                self.db_name = dd_uri["database"]
            else:
                self.host_uri = f"{self.host_uri}/{self.db_name}"

            try:
                self.connection = MongoClient(f"{self.host_uri}")
                self.db = self.connection[self.db_name]
            except Exception:
                logger.error("Mongodb connection failed")
                raise Exception
        else:
            try:

                self.connection = MongoClient(
                    host=self.host,
                    port=self.port,
                    username=self.user,
                    password=self.password,
                    **kwargs_unknown,
                )
                self.db = self.connection[self.db_name]
            except Exception:
                logger.error("Mongodb connection failed")
                raise Exception

        self.collection = self.db[cfg.collection]

        # set counter collection
        if self.db.counter.count_documents({"_id": "taskid"}) == 0:
            self.db.counter.insert_one({"_id": "taskid", "c": 0})
            self.build_indexes()

    @abstractmethod
    def build_indexes(self, indexes=None, background=True):
        """
        Build the indexes.

        Args:
            indexes (list): list of single field indexes to be built.
            background (bool): Run in the background or not.
        """

    def insert(self, d, update_duplicates=True):
        """
        Insert the task document to the database collection.

        Args:
            d (dict): task document
            update_duplicates (bool): whether to update the duplicates
        """
        result = self.collection.find_one(
            {"dir_name": d["dir_name"]}, ["dir_name", "task_id"]
        )
        if result is None or update_duplicates:
            d["last_updated"] = datetime.datetime.utcnow()
            if result is None:
                if ("task_id" not in d) or (not d["task_id"]):
                    d["task_id"] = self.db.counter.find_one_and_update(
                        {"_id": "taskid"},
                        {"$inc": {"c": 1}},
                        return_document=ReturnDocument.AFTER,
                    )["c"]
                logger.info(f"Inserting {d['dir_name']} with taskid = {d['task_id']}")
            elif update_duplicates:
                d["task_id"] = result["task_id"]
                logger.info(f"Updating {d['dir_name']} with taskid = {d['task_id']}")
            d = jsanitize(d, allow_bson=True)
            self.collection.update_one(
                {"dir_name": d["dir_name"]}, {"$set": d}, upsert=True
            )
            return d["task_id"]
        else:
            logger.info(f"Skipping duplicate {d['dir_name']}")
            return None

    @abstractmethod
    def reset(self):
        pass

    @classmethod
    def from_db_file(cls, db_file, admin=True):
        """
        Create MMDB from database file. File requires host, port, database,
        collection, and optionally admin_user/readonly_user and
        admin_password/readonly_password

        Args:
            db_file (str): path to the file containing the credentials
            admin (bool): whether to use the admin user

        Returns:
            MMDb object
        """
        creds = loadfn(db_file)
        cfg = DbConfigFile(**creds)

        kwargs = cfg.mongoclient_kwargs

        if cfg.host_uri:
            return cls(
                host_uri=cfg.host_uri,
                database=cfg.database,
                collection=cfg.collection,
                maggma_store_kwargs=cfg.maggma_store,
                maggma_store_prefix=cfg.maggma_store_prefix,
                **kwargs,
            )

        if admin and not cfg.admin_user and cfg.readonly_user:
            raise ValueError(
                "Trying to use admin credentials, "
                "but no admin credentials are defined. "
                "Use admin=False if only read_only "
                "credentials are available."
            )

        if admin:
            user = cfg.admin_user
            password = cfg.admin_password
        else:
            user = cfg.readonly_user
            password = cfg.readonly_password

        if "authsource" in creds:
            kwargs["authsource"] = cfg.authsource
        else:
            kwargs["authsource"] = cfg.database

        return cls(
            host=cfg.host,
            port=cfg.port,
            database=cfg.database,
            collection=cfg.collection,
            user=user,
            password=password,
            maggma_store_kwargs=cfg.maggma_store,
            maggma_store_prefix=cfg.maggma_store_prefix,
            **kwargs,
        )

    def get_store(self, store_name: str):
        """Get the maggma store with a specific name if it exists, if not create it first.

        Args:
            store_name : name of the store desired
        """
        if store_name not in self._maggma_stores:
            if self._maggma_store_type is None:
                logger.warn(
                    "The maggma store was requested but the maggma store type was not set.  Check your DB_FILE"
                )
                return None
            if self._maggma_store_type == "s3":
                self._maggma_stores[store_name] = self._get_s3_store(store_name)
            # Additional stores can be implemented here
            else:
                raise NotImplementedError("Maggma store type not currently supported.")
        return self._maggma_stores[store_name]

    def _get_s3_store(self, store_name):
        """
        Add a maggma store to this object for storage of large chunk data
        The maggma store will be stored to self.maggma_store[store_name]

        For aws store, all documents will be stored to the same bucket
        and the store_name will double as the sub_dir name.

        Args:
            store_name: correspond to the key within calcs_reversed.0 that will be stored
        """
        if self.host_uri is not None:
            index_store_ = MongoURIStore(
                uri=self.host_uri,
                database=self.db_name,
                collection_name=f"{self.maggma_store_prefix}_{store_name}_index",
                key="fs_id",
            )
        else:
            index_store_ = MongoStore(
                database=self.db_name,
                collection_name=f"{self.maggma_store_prefix}_{store_name}_index",
                host=self.host,
                port=self.port,
                username=self.user,
                password=self.password,
                key="fs_id",
            )

        store = S3Store(
            index=index_store_,
            sub_dir=f"{self.maggma_store_prefix}_{store_name}",
            key="fs_id",
            **self._maggma_store_kwargs,
        )

        return store
