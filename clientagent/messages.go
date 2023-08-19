package clientagent
const (
	CLIENT_LOGIN                                   = 1
    CLIENT_LOGIN_RESP                              = 2
    CLIENT_GET_AVATARS                             = 3
    // Sent by the server when it is dropping the connection deliberately.
    CLIENT_GO_GET_LOST                             = 4
    CLIENT_GET_AVATARS_RESP                        = 5
    CLIENT_CREATE_AVATAR                           = 6
    CLIENT_CREATE_AVATAR_RESP                      = 7
    CLIENT_GET_FRIEND_LIST                         = 10
    CLIENT_GET_FRIEND_LIST_RESP                    = 11
    CLIENT_GET_AVATAR_DETAILS                      = 14
    CLIENT_GET_AVATAR_DETAILS_RESP                 = 15
    CLIENT_LOGIN_2                                 = 16
    CLIENT_LOGIN_2_RESP                            = 17

    CLIENT_OBJECT_UPDATE_FIELD                     = 24
    CLIENT_OBJECT_UPDATE_FIELD_RESP                = 24
    CLIENT_OBJECT_DISABLE                          = 25
    CLIENT_OBJECT_DISABLE_RESP                     = 25
    CLIENT_OBJECT_DISABLE_OWNER                    = 26
    CLIENT_OBJECT_DISABLE_OWNER_RESP               = 26
    CLIENT_OBJECT_DELETE                           = 27
    CLIENT_OBJECT_DELETE_RESP                      = 27
    CLIENT_SET_ZONE_CMU                            = 29
    CLIENT_REMOVE_ZONE                             = 30
    CLIENT_SET_AVATAR                              = 32
    CLIENT_CREATE_OBJECT_REQUIRED                  = 34
    CLIENT_CREATE_OBJECT_REQUIRED_RESP             = 34
    CLIENT_CREATE_OBJECT_REQUIRED_OTHER            = 35
    CLIENT_CREATE_OBJECT_REQUIRED_OTHER_RESP       = 35
    CLIENT_CREATE_OBJECT_REQUIRED_OTHER_OWNER      = 36
    CLIENT_CREATE_OBJECT_REQUIRED_OTHER_OWNER_RESP = 36

    CLIENT_REQUEST_GENERATES                       = 36

    CLIENT_DISCONNECT                              = 37

    CLIENT_GET_STATE_RESP                          = 47
    CLIENT_DONE_INTEREST_RESP                      = 48

    CLIENT_DELETE_AVATAR                           = 49

    CLIENT_DELETE_AVATAR_RESP                      = 5

    CLIENT_HEARTBEAT                               = 52
    CLIENT_FRIEND_ONLINE                           = 53
    CLIENT_FRIEND_OFFLINE                          = 54
    CLIENT_REMOVE_FRIEND                           = 56

    CLIENT_CHANGE_PASSWORD                         = 65

    CLIENT_SET_NAME_PATTERN                        = 67
    CLIENT_SET_NAME_PATTERN_ANSWER                 = 68

    CLIENT_SET_WISHNAME                            = 70
    CLIENT_SET_WISHNAME_RESP                       = 71
    CLIENT_SET_WISHNAME_CLEAR                      = 72
    CLIENT_SET_SECURITY                            = 73

    CLIENT_SET_DOID_RANGE                          = 74

    CLIENT_GET_AVATARS_RESP2                       = 75
    CLIENT_CREATE_AVATAR2                          = 76
    CLIENT_SYSTEM_MESSAGE                          = 78
    CLIENT_SET_AVTYPE                              = 80

    CLIENT_GET_PET_DETAILS                         = 81
    CLIENT_GET_PET_DETAILS_RESP                    = 82

    CLIENT_ADD_INTEREST                            = 97
    CLIENT_REMOVE_INTEREST                         = 99
    CLIENT_OBJECT_LOCATION                         = 102

    CLIENT_LOGIN_3                                 = 111
    CLIENT_LOGIN_3_RESP                            = 110

    CLIENT_GET_FRIEND_LIST_EXTENDED                = 115
    CLIENT_GET_FRIEND_LIST_EXTENDED_RESP           = 116

    CLIENT_SET_FIELD_SENDABLE                      = 120

    CLIENT_SYSTEMMESSAGE_AKNOWLEDGE                = 123
    CLIENT_CHANGE_GENERATE_ORDER                   = 124

    // # new toontown specific login message, adds last logged in, and if child account has parent acount
    CLIENT_LOGIN_TOONTOWN                          = 125
    CLIENT_LOGIN_TOONTOWN_RESP                     = 126

    CLIENT_DISCONNECT_GENERIC                = 1
    CLIENT_DISCONNECT_RELOGIN                = 100
	CLIENT_DISCONNECT_OVERSIZED_DATAGRAM     = 106
	CLIENT_DISCONNECT_NO_HELLO               = 107
    CLIENT_DISCONNECT_CHAT_AUTH_ERROR        = 120
    CLIENT_DISCONNECT_ACCOUNT_ERROR          = 122
	CLIENT_DISCONNECT_NO_HEARTBEAT           = 345
	CLIENT_DISCONNECT_INVALID_MSGTYPE        = 108
	CLIENT_DISCONNECT_TRUNCATED_DATAGRAM     = 109
	CLIENT_DISCONNECT_ANONYMOUS_VIOLATION    = 113
	CLIENT_DISCONNECT_FORBIDDEN_INTEREST     = 115
	CLIENT_DISCONNECT_MISSING_OBJECT         = 117
	CLIENT_DISCONNECT_FORBIDDEN_FIELD        = 118
	CLIENT_DISCONNECT_FORBIDDEN_RELOCATE     = 119
	CLIENT_DISCONNECT_BAD_VERSION            = 125
	CLIENT_DISCONNECT_FIELD_CONSTRAINT       = 127
	CLIENT_DISCONNECT_SESSION_OBJECT_DELETED = 153
)
