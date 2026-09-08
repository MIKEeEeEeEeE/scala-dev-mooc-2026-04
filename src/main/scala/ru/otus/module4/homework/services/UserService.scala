package ru.otus.module4.homework.services

import io.getquill.context.ZioJdbc.QIO
import ru.otus.module4.homework.dao.entity.{Role, RoleCode, User, UserId}
import ru.otus.module4.homework.dao.repository.UserRepository
import ru.otus.module4.phoneBook.db
import zio.{ZIO, ZLayer}

trait UserService{
    def listUsers(): QIO[List[User]]
    def listUsersDTO(): QIO[List[UserDTO]]
    def addUserWithRole(user: User, roleCode: RoleCode): QIO[UserDTO]
    def listUsersWithRole(roleCode: RoleCode): QIO[List[UserDTO]]
}
class Impl(userRepo: UserRepository) extends UserService {
    val dc = db.Ctx

    def listUsers(): QIO[List[User]] =
        userRepo.list()

    def listUsersDTO(): QIO[List[UserDTO]] = listUsers().flatMap { users =>
        ZIO.foreach(users) { user =>
            userRepo
              .userRoles(UserId(user.id))
              .map { roles =>
                  UserDTO(user, roles.toSet)
              }
        }
    }

    def addUserWithRole(user: User, roleCode: RoleCode): QIO[UserDTO] = for {
        userQIO <- userRepo.createUser(user)
        userId = UserId(userQIO.id)
        _ <- userRepo.insertRoleToUser(roleCode, userId)
        rolesQIO <- userRepo.userRoles(userId)
    } yield UserDTO(userQIO, rolesQIO.toSet)

    def listUsersWithRole(roleCode: RoleCode): QIO[List[UserDTO]] = listUsers().flatMap { users =>
        ZIO.foreach(users) { user =>
            userRepo
              .userRoles(UserId(user.id))
              .map( roles =>
                  if (roles.exists(_.code == roleCode.code))
                      Some(UserDTO(user, roles.toSet))
                  else
                      None
              )
        }.map(_.flatten)
    }


}
object UserService{

    val layer: ZLayer[UserRepository, Nothing, UserService] = ZLayer.fromFunction(new Impl(_))
}

case class UserDTO(user: User, roles: Set[Role])