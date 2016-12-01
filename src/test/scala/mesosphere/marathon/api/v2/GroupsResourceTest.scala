package mesosphere.marathon
package api.v2

import java.util.Collections

import mesosphere.marathon.api.v2.json.Formats._
import mesosphere.marathon.api.{ TestAuthFixture, TestGroupManagerFixture }
import mesosphere.marathon.core.appinfo._
import mesosphere.marathon.core.group.GroupManager
import mesosphere.marathon.raml.{ App, GroupUpdate }
import mesosphere.marathon.state.PathId._
import mesosphere.marathon.state._
import mesosphere.marathon.storage.repository.GroupRepository
import mesosphere.marathon.test.{ GroupCreation, MarathonSpec, Mockito }
import org.scalatest.{ GivenWhenThen, Matchers }
import play.api.libs.json.{ JsObject, Json }

import scala.concurrent.Future

class GroupsResourceTest extends MarathonSpec with Matchers with Mockito with GivenWhenThen with GroupCreation {
  test("dry run update") {
    Given("A real Group Manager with no groups")
    useRealGroupManager()
    groupRepository.root() returns Future.successful(createRootGroup())

    val app = App(id = "/test/app", cmd = Some("test cmd"))
    val update = GroupUpdate(id = Some("/test"), apps = Some(Set(app)))

    When("Doing a dry run update")
    val body = Json.stringify(Json.toJson(update)).getBytes
    val result = groupsResource.update("/test", force = false, dryRun = true, body, auth.request)
    val json = Json.parse(result.getEntity.toString)

    Then("The deployment plan is correct")
    val steps = (json \ "steps").as[Seq[JsObject]]
    assert(steps.size == 2)

    val firstStep = (steps.head \ "actions").as[Seq[JsObject]].head
    assert((firstStep \ "action").as[String] == "StartApplication")
    assert((firstStep \ "app").as[String] == "/test/app")

    val secondStep = (steps.last \ "actions").as[Seq[JsObject]].head
    assert((secondStep \ "action").as[String] == "ScaleApplication")
    assert((secondStep \ "app").as[String] == "/test/app")
  }

  test("dry run update on an existing group") {
    Given("A real Group Manager with no groups")
    useRealGroupManager()
    val rootGroup = createRootGroup().makeGroup(PathId("/foo/bla"))
    groupRepository.root() returns Future.successful(rootGroup)

    val app = App(id = "/foo/bla/app", cmd = Some("test cmd"))
    val update = GroupUpdate(id = Some("/foo/bla"), apps = Some(Set(app)))

    When("Doing a dry run update")
    val body = Json.stringify(Json.toJson(update)).getBytes
    val result = groupsResource.update("/foo/bla", force = false, dryRun = true, body, auth.request)
    val json = Json.parse(result.getEntity.toString)

    Then("The deployment plan is correct")
    val steps = (json \ "steps").as[Seq[JsObject]]
    assert(steps.size == 2)

    val firstStep = (steps.head \ "actions").as[Seq[JsObject]].head
    assert((firstStep \ "action").as[String] == "StartApplication")
    assert((firstStep \ "app").as[String] == "/foo/bla/app")

    val secondStep = (steps.last \ "actions").as[Seq[JsObject]].head
    assert((secondStep \ "action").as[String] == "ScaleApplication")
    assert((secondStep \ "app").as[String] == "/foo/bla/app")
  }

  test("access without authentication is denied") {
    Given("An unauthenticated request")
    auth.authenticated = false
    val req = auth.request
    val body = """{"id":"/a/b/c","cmd":"foo","ports":[]}"""

    groupManager.rootGroup() returns Future.successful(createRootGroup())

    When("the root is fetched from index")
    val root = groupsResource.root(req, embed)
    Then("we receive a NotAuthenticated response")
    root.getStatus should be(auth.NotAuthenticatedStatus)

    When("the group by id is fetched from create")
    val rootGroup = groupsResource.group("/foo/bla", embed, req)
    Then("we receive a NotAuthenticated response")
    rootGroup.getStatus should be(auth.NotAuthenticatedStatus)

    When("the root group is created")
    val create = groupsResource.create(false, body.getBytes("UTF-8"), req)
    Then("we receive a NotAuthenticated response")
    create.getStatus should be(auth.NotAuthenticatedStatus)

    When("the group is created")
    val createWithPath = groupsResource.createWithPath("/my/id", false, body.getBytes("UTF-8"), req)
    Then("we receive a NotAuthenticated response")
    createWithPath.getStatus should be(auth.NotAuthenticatedStatus)

    When("the root group is updated")
    val updateRoot = groupsResource.updateRoot(false, false, body.getBytes("UTF-8"), req)
    Then("we receive a NotAuthenticated response")
    updateRoot.getStatus should be(auth.NotAuthenticatedStatus)

    When("the group is updated")
    val update = groupsResource.update("", false, false, body.getBytes("UTF-8"), req)
    Then("we receive a NotAuthenticated response")
    update.getStatus should be(auth.NotAuthenticatedStatus)

    When("the root group is deleted")
    val deleteRoot = groupsResource.delete(false, req)
    Then("we receive a NotAuthenticated response")
    deleteRoot.getStatus should be(auth.NotAuthenticatedStatus)

    When("the group is deleted")
    val delete = groupsResource.delete("", false, req)
    Then("we receive a NotAuthenticated response")
    delete.getStatus should be(auth.NotAuthenticatedStatus)
  }

  test("access without authorization is denied if the resource exists") {
    Given("A real group manager with one app")
    useRealGroupManager()
    val app = AppDefinition("/a".toRootPath)
    val rootGroup = createRootGroup(apps = Map(app.id -> app))
    groupRepository.root() returns Future.successful(rootGroup)

    Given("An unauthorized request")
    auth.authenticated = true
    auth.authorized = false
    val req = auth.request
    val body = """{"id":"/a/b/c","cmd":"foo","ports":[]}"""

    When("the root group is created")
    val create = groupsResource.create(false, body.getBytes("UTF-8"), req)
    Then("we receive a Not Authorized response")
    create.getStatus should be(auth.UnauthorizedStatus)

    When("the group is created")
    val createWithPath = groupsResource.createWithPath("/my/id", false, body.getBytes("UTF-8"), req)
    Then("we receive a Not Authorized response")
    createWithPath.getStatus should be(auth.UnauthorizedStatus)

    When("the root group is updated")
    val updateRoot = groupsResource.updateRoot(false, false, body.getBytes("UTF-8"), req)
    Then("we receive a Not Authorized response")
    updateRoot.getStatus should be(auth.UnauthorizedStatus)

    When("the group is updated")
    val update = groupsResource.update("", false, false, body.getBytes("UTF-8"), req)
    Then("we receive a Not Authorized response")
    update.getStatus should be(auth.UnauthorizedStatus)

    When("the root group is deleted")
    val deleteRoot = groupsResource.delete(false, req)
    Then("we receive a Not Authorized response")
    deleteRoot.getStatus should be(auth.UnauthorizedStatus)

    When("the group is deleted")
    val delete = groupsResource.delete("", false, req)
    Then("we receive a Not Authorized response")
    delete.getStatus should be(auth.UnauthorizedStatus)
  }

  test("access to root group without authentication is allowed") {
    Given("An unauthenticated request")
    auth.authenticated = true
    auth.authorized = false
    val req = auth.request
    groupInfo.selectGroup(any, any, any, any) returns Future.successful(None)

    When("the root is fetched from index")
    val root = groupsResource.root(req, embed)

    Then("the request is successful")
    root.getStatus should be(200)
  }

  test("authenticated delete without authorization leads to a 404 if the resource doesn't exist") {
    Given("A real group manager with no apps")
    useRealGroupManager()
    groupRepository.root() returns Future.successful(createRootGroup())

    Given("An unauthorized request")
    auth.authenticated = true
    auth.authorized = false
    val req = auth.request

    When("the group is deleted")
    Then("we get a 404")
    // FIXME (gkleiman): this leads to an ugly stack trace
    intercept[UnknownGroupException] { groupsResource.delete("/foo", false, req) }
  }

  test("Group Versions for root are transferred as simple json string array (Fix #2329)") {
    Given("Specific Group versions")
    val groupVersions = Seq(Timestamp.now(), Timestamp.now())
    groupManager.versions(PathId.empty) returns Future.successful(groupVersions)
    groupManager.group(PathId.empty) returns Future.successful(Some(createGroup(PathId.empty)))

    When("The versions are queried")
    val rootVersionsResponse = groupsResource.group("versions", embed, auth.request)

    Then("The versions are send as simple json array")
    rootVersionsResponse.getStatus should be (200)
    rootVersionsResponse.getEntity should be(Json.toJson(groupVersions).toString())
  }

  test("Group Versions for path are transferred as simple json string array (Fix #2329)") {
    Given("Specific group versions")
    val groupVersions = Seq(Timestamp.now(), Timestamp.now())
    groupManager.versions(any) returns Future.successful(groupVersions)
    groupManager.versions("/foo/bla/blub".toRootPath) returns Future.successful(groupVersions)
    groupManager.group("/foo/bla/blub".toRootPath) returns Future.successful(Some(createGroup("/foo/bla/blub".toRootPath)))

    When("The versions are queried")
    val rootVersionsResponse = groupsResource.group("/foo/bla/blub/versions", embed, auth.request)

    Then("The versions are send as simple json array")
    rootVersionsResponse.getStatus should be (200)
    rootVersionsResponse.getEntity should be(Json.toJson(groupVersions).toString())
  }

  test("Creation of a group with same path as an existing app should be prohibited (fixes #3385)") {
    Given("A real group manager with one app")
    useRealGroupManager()
    val app = AppDefinition("/group/app".toRootPath)
    val rootGroup = createRootGroup(groups = Set(createGroup("/group".toRootPath, Map(app.id -> app))))
    groupRepository.root() returns Future.successful(rootGroup)

    When("creating a group with the same path existing app")
    val body = Json.stringify(Json.toJson(GroupUpdate(id = Some("/group/app"))))

    Then("we get a 409")
    intercept[ConflictingChangeException] { groupsResource.create(false, body.getBytes, auth.request) }
  }

  test("Creation of a group with same path as an existing group should be prohibited") {
    Given("A real group manager with one app")
    useRealGroupManager()
    val rootGroup = createRootGroup(groups = Set(createGroup("/group".toRootPath)))
    groupRepository.root() returns Future.successful(rootGroup)

    When("creating a group with the same path existing app")
    val body = Json.stringify(Json.toJson(GroupUpdate(id = Some("/group"))))

    Then("we get a 409")
    intercept[ConflictingChangeException] { groupsResource.create(false, body.getBytes, auth.request) }
  }

  var config: MarathonConf = _
  var groupManager: GroupManager = _
  var groupRepository: GroupRepository = _
  var groupsResource: GroupsResource = _
  var auth: TestAuthFixture = _
  var groupInfo: GroupInfoService = _
  val embed: java.util.Set[String] = Collections.emptySet()

  before {
    auth = new TestAuthFixture
    config = AllConf.withTestConfig("--zk_timeout", "10000") // 10s seems reasonable on a busy CI system
    groupManager = mock[GroupManager]
    groupInfo = mock[GroupInfoService]
    groupsResource = new GroupsResource(groupManager, groupInfo, config)(auth.auth, auth.auth)
  }

  private[this] def useRealGroupManager(): Unit = {
    val f = new TestGroupManagerFixture()
    config = f.config
    groupRepository = f.groupRepository
    groupManager = f.groupManager

    groupsResource = new GroupsResource(groupManager, groupInfo, config)(auth.auth, auth.auth)
  }
}
