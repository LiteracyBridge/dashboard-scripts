BEGIN TRANSACTION;
delete from topmsgsbypkgall_s;
insert into topmsgsbypkgall_s select * from topmsgsbypkgall;
COMMIT;
