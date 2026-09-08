package ru.otus.module4.homework.dao.repository

import zio.{ULayer, ZLayer}
import io.getquill.context.ZioJdbc.*
import io.getquill.*
import ru.otus.module4.homework.dao.entity.*
import ru.otus.module4.phoneBook.db

trait UserRepository{
    def findUser(userId: UserId): QIO[Option[User]]
    def createUser(user: User): QIO[User]
    def createUsers(users: List[User]): QIO[List[User]]
    def updateUser(user: User): QIO[Unit]
    def deleteUser(user: User): QIO[Unit]
    def findByLastName(lastName: String): QIO[List[User]]
    def list(): QIO[List[User]]
    def userRoles(userId: UserId): QIO[List[Role]]
    def insertRoleToUser(roleCode: RoleCode, userId: UserId): QIO[Unit]
    def listUsersWithRole(roleCode: RoleCode): QIO[List[User]]
    def findRoleByCode(roleCode: RoleCode): QIO[Option[Role]]
}


class UserRepositoryImpl extends UserRepository {
    val dc = db.Ctx
    import dc._

    override def findUser(userId: UserId): QIO[Option[User]] = run(query[User].filter(_.id == lift(userId.id))).map(_.headOption)

    override def createUser(user: User): QIO[User] = run(query[User].insertValue(lift(user))).as(user)

    override def createUsers(users: List[User]): QIO[List[User]] = run(liftQuery(users).foreach(query[User].insertValue(_))).as(users)

    override def updateUser(user: User): QIO[Unit] = run(query[User].updateValue(lift(user))).unit

    override def deleteUser(user: User): QIO[Unit] = run(query[User].filter(_ == lift(user)).delete).unit

    override def findByLastName(lastName: String): QIO[List[User]] = run(query[User].filter(_.lastName == lift(lastName)))

    override def list(): QIO[List[User]] = run(query[User])

    override def userRoles(userId: UserId): QIO[List[Role]] = run { for {
            a <- query[User] if a.id == lift(userId.id)
            b <- query[UserToRole] if a.id == b.userId
            c <- query[Role] if b.roleId == c.code
        } yield c
    }
    
    override def insertRoleToUser(roleCode: RoleCode, userId: UserId): QIO[Unit] = 
        run(query[UserToRole].insertValue(lift(UserToRole(roleCode.code, userId.id)))).unit

    override def listUsersWithRole(roleCode: RoleCode): QIO[List[User]] = run { for {
            a <- query[Role] if a.code == lift(roleCode.code)
            b <- query[UserToRole] if b.roleId == a.code
            c <- query[User] if c.id == b.userId
        } yield c
    }

    override def findRoleByCode(roleCode: RoleCode): QIO[Option[Role]] = run(query[Role].filter(_.code == lift(roleCode.code))).map(_.headOption)
}

object UserRepository{

    val layer: ULayer[UserRepository] = ZLayer.succeed(new UserRepositoryImpl)
}